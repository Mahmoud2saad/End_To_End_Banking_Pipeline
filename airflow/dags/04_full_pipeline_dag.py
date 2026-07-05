# dags/04_full_pipeline_dag.py — Full end-to-end pipeline (daily at 02:00 UTC)
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from operators.dbt_operator import DbtOperator

PIPELINE_HOME    = os.getenv("BANKING_PIPELINE_HOME", "/opt/airflow/project")
DBT_DIR          = f"{PIPELINE_HOME}/banking_dbt"
ALERT_EMAIL      = os.getenv("ALERT_EMAIL", "Mahmoud0Saad@outlook.com")

SPARK_ENV = {
    "BANKING_PIPELINE_HOME": PIPELINE_HOME,
    "SPARK_MASTER": "local[*]",
    "DUCKDB_PATH": os.getenv("DUCKDB_PATH", f"{PIPELINE_HOME}/local_warehouse/banking.duckdb"),
    "SILVER_PATH": os.getenv("SILVER_PATH", f"{PIPELINE_HOME}/local_warehouse/delta/silver"),
    "JAVA_HOME": os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"),
    "PATH": os.environ.get("PATH", ""),
}

def on_failure_callback(context):
    from airflow.utils.email import send_email
    task_id = context['task_instance'].task_id
    send_email(
        to=ALERT_EMAIL,
        subject=f"❌ [04_full_pipeline] FAILED at: {task_id}",
        html_content=f"""
        <h2 style="color:red;">🚨 Banking Pipeline Daily Run FAILED</h2>
        <table border="1" cellpadding="6">
          <tr><td><b>Failed task</b></td><td style="color:red;"><b>{task_id}</b></td></tr>
          <tr><td><b>Run date</b></td><td>{context['ds']}</td></tr>
          <tr><td><b>Error</b></td><td>{context.get('exception','Unknown')}</td></tr>
          <tr><td><b>Logs</b></td><td><a href="{context['task_instance'].log_url}">View logs</a></td></tr>
        </table>
        <br>
        <p><b>Pipeline flow:</b><br>
        Bronze → Silver → Register Views → dbt Seed → dbt Snapshot
        → dbt Run → dbt Test → <b style="color:red;">✗ {task_id}</b></p>
        <p>⚠️ Please fix and re-trigger the DAG manually.</p>
        """,
    )

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="04_full_pipeline",
    description="End-to-end: Bronze → Silver → dbt Gold (daily 02:00 UTC)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=["banking", "full-pipeline"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"python {PIPELINE_HOME}/notebooks/local/01_bronze_local.py",
        env=SPARK_ENV,
    )

    silver = BashOperator(
        task_id="silver_transformation",
        bash_command=f"python {PIPELINE_HOME}/notebooks/local/02_silver_local.py",
        env=SPARK_ENV,
    )

    register_views = BashOperator(
        task_id="register_silver_views",
        bash_command=f"python {PIPELINE_HOME}/scripts/register_silver_views.py",
        env=SPARK_ENV,
    )

    dbt_seed     = DbtOperator(task_id="dbt_seed",     command="seed",     profiles_dir=DBT_DIR, project_dir=DBT_DIR)
    dbt_snapshot = DbtOperator(task_id="dbt_snapshot", command="snapshot", profiles_dir=DBT_DIR, project_dir=DBT_DIR)
    dbt_run      = DbtOperator(task_id="dbt_run_all",  command="run",      profiles_dir=DBT_DIR, project_dir=DBT_DIR, full_refresh=True)
    dbt_test     = DbtOperator(task_id="dbt_test",     command="test",     profiles_dir=DBT_DIR, project_dir=DBT_DIR)

    notify_success = EmailOperator(
        task_id="notify_success",
        to=ALERT_EMAIL,
        subject="✅ [04_full_pipeline] Banking pipeline completed successfully",
        html_content="""
        <h2 style="color:green;">🏦 Banking Pipeline — Daily Run Complete</h2>
        <p>Full end-to-end pipeline finished without errors on <b>{{ ds }}</b>.</p>
        <ul>
          <li>✔ Bronze — landing zone ingested</li>
          <li>✔ Silver — data cleaned and typed (8 tables)</li>
          <li>✔ Silver views registered in DuckDB</li>
          <li>✔ dbt seed — reference tables loaded</li>
          <li>✔ dbt snapshot — SCD2 updated</li>
          <li>✔ dbt run — all 25 models built</li>
          <li>✔ dbt test — all 202 data quality tests passed</li>
        </ul>
        <p>Gold layer is ready for reporting and analysis.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # This fires if ANY task in the pipeline fails
    notify_failure = EmailOperator(
        task_id="notify_pipeline_failure",
        to=ALERT_EMAIL,
        subject="❌ [04_full_pipeline] Pipeline FAILED — check on_failure emails for details",
        html_content="""
        <h2 style="color:red;">Banking Pipeline Daily Run FAILED</h2>
        <p>Run date: <b>{{ ds }}</b></p>
        <p>Check the individual task failure emails sent earlier for the
           specific error and log link.</p>
        <p>The Gold layer data may be incomplete or stale for today.</p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    # Main pipeline chain
    (
        start
        >> bronze >> silver >> register_views
        >> dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
        >> notify_success
        >> end
    )

    # Failure summary fires if anything above fails
    [bronze, silver, register_views,
     dbt_seed, dbt_snapshot, dbt_run, dbt_test] >> notify_failure