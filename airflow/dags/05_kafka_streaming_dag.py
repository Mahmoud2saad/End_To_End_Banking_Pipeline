"""
airflow/dags/05_kafka_streaming_dag.py

Brings Kafka streaming into Airflow's orchestration -- previously the
producer/consumer only ran as manual terminal commands, invisible to
Airflow entirely.

Design decision worth understanding, not just accepting: the CONSUMER is
NOT a task in this DAG. A Kafka consumer is meant to run continuously;
running it as an Airflow task would occupy a worker slot forever and
isn't what Airflow tasks are for. Instead, the consumer runs as an
always-on Docker service (kafka/docker-compose.yml's new kafka-consumer
service, with restart:unless-stopped) -- the same pattern the broker
itself already uses. This DAG's job is to VERIFY that service is alive
before producing anything, and to run the PRODUCER (which does complete,
so it fits Airflow's task model correctly).

Task flow:
    check_kafka_consumer_alive  ->  run_kafka_producer

If the consumer isn't running, this DAG fails loudly at the health check
instead of the producer silently streaming data into topics nobody is
consuming -- which would look like success in Airflow while actually
producing 0 rows in the landing zone.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def check_kafka_consumer_alive(**context):
    """
    Verifies the kafka-consumer service's consumer group actually has an
    active member -- not just that the container is running, since a
    crashed-but-restarting container could still show as "Up" in Docker
    while the consumer process inside keeps failing.

    Uses kafka-python directly (same library the producer/consumer already
    depend on) rather than going through grafana_api's /kafka-lag endpoint,
    so this check doesn't have an extra dependency on grafana_api also
    being up.
    """
    import os
    from kafka.admin import KafkaAdminClient
    from kafka.errors import NoBrokersAvailable

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    consumer_group = os.environ.get("KAFKA_CONSUMER_GROUP", "bronze-ingestion-consumer")

    try:
        admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        groups = admin.list_consumer_groups()
        group_ids = [g[0] for g in groups]
        admin.close()
    except NoBrokersAvailable as e:
        raise RuntimeError(
            f"Cannot reach Kafka broker at {bootstrap_servers}. "
            f"Is `docker compose up -d` running in kafka/? Original error: {e}"
        )

    if consumer_group not in group_ids:
        raise RuntimeError(
            f"Consumer group '{consumer_group}' has no active members. "
            f"The kafka-consumer service may have crashed or never started. "
            f"Check: docker compose -f kafka/docker-compose.yml ps kafka-consumer "
            f"and docker compose -f kafka/docker-compose.yml logs kafka-consumer"
        )

    print(f"Consumer group '{consumer_group}' is alive and registered with the broker.")


default_args = {
    "owner": "banking_pipeline",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="05_kafka_streaming",
    description="Verifies the Kafka consumer service is alive, then runs the producer",
    default_args=default_args,
    schedule=None,  # manually triggered -- see DAG's docstring for why the
                     # consumer itself isn't scheduled/run as a task here
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "streaming"],
) as dag:

    check_consumer = PythonOperator(
        task_id="check_kafka_consumer_alive",
        python_callable=check_kafka_consumer_alive,
    )

    run_producer = BashOperator(
        task_id="run_kafka_producer",
        # BANKING_PIPELINE_HOME is a plain OS environment variable (set in
        # docker-compose.yml, same as the other DAGs in this repo already
        # use) -- NOT an Airflow Variable, so this reads it via bash's own
        # substitution, not Jinja's {{ var.value }} (a different, metadata-
        # DB-backed concept that would silently resolve to something else).
        bash_command=(
            "cd \"${BANKING_PIPELINE_HOME:-/opt/airflow}\" && "
            "python kafka/producer/generate_events.py"
        ),
        # Bounded by design: SIMULATOR_MODE=demo (the .env default) caps
        # rows per table, so this task actually completes rather than
        # running forever -- unlike the consumer, which is why the
        # consumer is a docker-compose service instead of a task here.
        execution_timeout=timedelta(minutes=30),
    )

    check_consumer >> run_producer
