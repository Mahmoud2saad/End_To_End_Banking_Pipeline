# dags/00_generate_data_dag.py
"""
DAG 0 — Data Generation (manual trigger only)
Runs the stream simulator to populate the landing zone.
"""
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
from operators.spark_operator import SparkSubmitLocalOperator

PIPELINE_HOME = os.getenv("BANKING_PIPELINE_HOME", "/opt/airflow/project")
ALERT_EMAIL   = os.getenv("ALERT_EMAIL", "Mahmoud0Saad@outlook.com")

def on_failure_callback(context):
    from airflow.utils.email import send_email
    send_email(
        to=ALERT_EMAIL,
        subject=f"❌ [00_generate_data] FAILED: {context['task_instance'].task_id}",
        html_content=f"""
        <h2 style="color:red;">Data Generation Failed</h2>
        <table border="1" cellpadding="6">
          <tr><td><b>Task</b></td><td>{context['task_instance'].task_id}</td></tr>
          <tr><td><b>Run date</b></td><td>{context['ds']}</td></tr>
          <tr><td><b>Error</b></td><td>{context.get('exception','Unknown')}</td></tr>
          <tr><td><b>Logs</b></td><td><a href="{context['task_instance'].log_url}">View logs</a></td></tr>
        </table>
        """,
    )

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="00_generate_data",
    description="Run stream simulator to populate landing zone (manual trigger)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["banking", "simulation"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    simulate = SparkSubmitLocalOperator(
        task_id="run_stream_simulator",
        script_path=f"{PIPELINE_HOME}/data_simulation/stream_simulator.py",
        script_args=["--delay", "0", "--max-batches", "5"],
        env={
            "BANKING_PIPELINE_HOME": PIPELINE_HOME,
            "JAVA_HOME": os.getenv("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"),
            "PATH": os.environ.get("PATH", ""),
        },
    )

    notify_success = EmailOperator(
        task_id="notify_success",
        to=ALERT_EMAIL,
        subject="✅ [00_generate_data] Simulation completed",
        html_content="""
        <h2 style="color:green;">Data Simulation Complete</h2>
        <p>Stream simulator finished writing to the landing zone on <b>{{ ds }}</b>.</p>
        <p>You can now trigger <b>04_full_pipeline</b>.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    start >> simulate >> notify_success >> end