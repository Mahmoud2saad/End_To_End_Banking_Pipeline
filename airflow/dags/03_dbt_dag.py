# dags/03_dbt_dag.py — dbt Gold Layer
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.bash import BashOperator
from operators.dbt_operator import DbtOperator
PIPELINE_HOME    = os.getenv("BANKING_PIPELINE_HOME", "/opt/airflow/project")
DBT_DIR          = f"{PIPELINE_HOME}/banking_dbt"
ALERT_EMAIL      = os.getenv("ALERT_EMAIL", "Mahmoud0Saad@outlook.com")

def on_failure_callback(context):
    from airflow.utils.email import send_email
    send_email(
        to=ALERT_EMAIL,
        subject=f"❌ [03_dbt_gold] FAILED: {context['task_instance'].task_id}",
        html_content=f"""
        <h2 style="color:red;">dbt Gold Layer Failed</h2>
        <table border="1" cellpadding="6">
          <tr><td><b>Task</b></td><td>{context['task_instance'].task_id}</td></tr>
          <tr><td><b>Run date</b></td><td>{context['ds']}</td></tr>
          <tr><td><b>Error</b></td><td>{context.get('exception','Unknown')}</td></tr>
          <tr><td><b>Logs</b></td><td><a href="{context['task_instance'].log_url}">View logs</a></td></tr>
        </table>
        <p>⚠️ Gold layer tables may be stale. Check dbt logs for model details.</p>
        """,
    )

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email": [ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "on_failure_callback": on_failure_callback,
}

with DAG(
    dag_id="03_dbt_gold",
    description="Build Gold layer via dbt (dimensions, facts, analytics)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="30 * * * *",
    catchup=False,
    tags=["banking", "dbt", "gold"],
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver",
        external_dag_id="02_silver_transformation",
        external_task_id="end",
        timeout=600,
        poke_interval=30,
        mode="reschedule",
    )

    dbt_seed     = DbtOperator(task_id="dbt_seed",         command="seed",     profiles_dir=DBT_DIR, project_dir=DBT_DIR)
    dbt_snapshot = DbtOperator(task_id="dbt_snapshot",     command="snapshot", profiles_dir=DBT_DIR, project_dir=DBT_DIR)
    dbt_staging  = DbtOperator(task_id="dbt_run_staging",  command="run",      profiles_dir=DBT_DIR, project_dir=DBT_DIR, select="tag:staging")
    dbt_marts    = DbtOperator(task_id="dbt_run_marts",    command="run",      profiles_dir=DBT_DIR, project_dir=DBT_DIR, select="tag:marts")
    dbt_test     = DbtOperator(task_id="dbt_test",         command="test",     profiles_dir=DBT_DIR, project_dir=DBT_DIR)

    notify_success = EmailOperator(
        task_id="notify_success",
        to=ALERT_EMAIL,
        subject="✅ [03_dbt_gold] Gold layer built and validated",
        html_content="""
        <h2 style="color:green;">dbt Gold Layer Complete</h2>
        <p>All dbt models built and all data quality tests passed on <b>{{ ds }}</b>.</p>
        <table border="1" cellpadding="6">
          <tr><td><b>Steps</b></td><td>seed → snapshot → staging → marts → tests</td></tr>
          <tr><td><b>Dimensions</b></td><td>dim_customer, dim_card, dim_atm, dim_date, dim_geography, dim_merchant</td></tr>
          <tr><td><b>Facts</b></td><td>fact_atm_transactions, fact_card_transactions, fact_wallet_transactions, fact_out_of_cash_events</td></tr>
          <tr><td><b>Gold</b></td><td>atm_performance, fraud_risk_scoring, customer_spending_behavior, replenishment_analysis, channel_comparison, governorate_summary</td></tr>
        </table>
        <p>✔ Gold layer is ready for reporting.</p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (
        start
        >> wait_for_silver
        >> dbt_seed >> dbt_snapshot >> dbt_staging >> dbt_marts >> dbt_test
        >> notify_success
        >> end
    )