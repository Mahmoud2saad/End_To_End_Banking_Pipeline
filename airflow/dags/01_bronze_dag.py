# dags/01_bronze_dag.py — Bronze Ingestion (every 30 min)
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

PIPELINE_HOME = os.getenv("BANKING_PIPELINE_HOME", "/opt/airflow/project")
ALERT_EMAIL   = os.getenv("ALERT_EMAIL", "Mahmoud0Saad@outlook.com")

SPARK_ENV = {
    "BANKING_PIPELINE_HOME": PIPELINE_HOME,
    "SPARK_MASTER": "local[*]",
    "JAVA_HOME": os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"),
    "PATH": os.environ.get("PATH", ""),
}

def on_failure_callback(context):
    from airflow.utils.email import send_email
    send_email(
        to=ALERT_EMAIL,
        subject=f"❌ [01_bronze_ingestion] FAILED: {context['task_instance'].task_id}",
        html_content=f"""
        <h2 style="color:red;">Bronze Ingestion Failed</h2>
        <table border="1" cellpadding="6">
          <tr><td><b>Task</b></td><td>{context['task_instance'].task_id}</td></tr>
          <tr><td><b>Run date</b></td><td>{context['ds']}</td></tr>
          <tr><td><b>Error</b></td><td>{context.get('exception','Unknown')}</td></tr>
          <tr><td><b>Logs</b></td><td><a href="{context['task_instance'].log_url}">View logs</a></td></tr>
        </table>
        <p>⚠️ Silver and Gold layers will be blocked until this is fixed.</p>
        """,
    )

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="01_bronze_ingestion",
    description="Ingest landing zone Parquet files into Bronze layer",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="*/30 * * * *",
    catchup=False,
    tags=["banking", "bronze"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    bronze = BashOperator(
        task_id="run_bronze_pipeline",
        bash_command=f"python {PIPELINE_HOME}/notebooks/local/01_bronze_local.py",
        env=SPARK_ENV,
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to=ALERT_EMAIL,
        subject="✅ [01_bronze_ingestion] Bronze layer loaded",
        html_content="""
        <h2 style="color:green;">Bronze Ingestion Complete</h2>
        <p>Landing zone files consolidated into Bronze on <b>{{ ds }}</b>.</p>
        <p>Silver transformation will run next.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> bronze >> notify_success >> end