# dags/00_test_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator

with DAG(
    dag_id="00_test",
    start_date=datetime(2026,6,6),
    schedule_interval=None,
    catchup=False,
) as dag:
    EmptyOperator(task_id="hello")
