# dags/02_silver_dag.py — Silver Transformation
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
        subject=f"❌ [02_silver_transformation] FAILED: {context['task_instance'].task_id}",
        html_content=f"""
        <h2 style="color:red;">Silver Transformation Failed</h2>
        <table border="1" cellpadding="6">
          <tr><td><b>Task</b></td><td>{context['task_instance'].task_id}</td></tr>
          <tr><td><b>Run date</b></td><td>{context['ds']}</td></tr>
          <tr><td><b>Error</b></td><td>{context.get('exception','Unknown')}</td></tr>
          <tr><td><b>Logs</b></td><td><a href="{context['task_instance'].log_url}">View logs</a></td></tr>
        </table>
        <p>⚠️ dbt Gold layer will be blocked until this is fixed.</p>
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
    dag_id="02_silver_transformation",
    description="Transform Bronze → Silver (clean Delta tables)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="15,45 * * * *",
    catchup=False,
    tags=["banking", "silver"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="01_bronze_ingestion",
        external_task_id="end",
        timeout=600,
        poke_interval=30,
        mode="reschedule",
    )

    silver = BashOperator(
        task_id="run_silver_pipeline",
        bash_command=f"python {PIPELINE_HOME}/notebooks/local/02_silver_local.py",
        env=SPARK_ENV,
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to=ALERT_EMAIL,
        subject="✅ [02_silver_transformation] Silver layer ready",
        html_content="""
        <h2 style="color:green;">Silver Transformation Complete</h2>
        <p>Bronze cleaned → Silver Delta tables on <b>{{ ds }}</b>.</p>
        <b>Tables:</b> atm_master, customers, cards, card_transactions,
        wallet_transactions, out_of_cash, kaggle_transactions, pan_customer_map.
        <p>dbt Gold build will run next.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> wait_for_bronze >> silver >> notify_success >> end