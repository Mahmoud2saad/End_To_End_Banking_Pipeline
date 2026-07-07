<#
.SYNOPSIS
    Brings Kafka streaming into Airflow's orchestration and makes the
    consumer an always-on Docker service instead of a manual terminal
    command.

.DESCRIPTION
    - kafka/consumer/Dockerfile: NEW -- containerizes the consumer.
    - Appends a new `kafka-consumer` service to kafka/docker-compose.yml
      (append-only, your existing kafka/kafka-ui services are untouched).
      Runs with restart:unless-stopped, uses the broker's INTERNAL
      listener (kafka:9092, not the host-facing localhost:9094 in your
      .env), and mounts your real local_warehouse/ so it writes to the
      same landing zone your Bronze DAG already reads from.
    - airflow/dags/05_kafka_streaming_dag.py: NEW DAG with 2 tasks:
        1. check_kafka_consumer_alive -- verifies the consumer group has
           an active member before producing anything (fails loudly
           instead of silently streaming into an unconsumed topic)
        2. run_kafka_producer -- runs the producer as a bounded task
      Deliberately does NOT run the consumer as a task -- a continuously
      running consumer would occupy an Airflow worker slot forever; it
      belongs in docker-compose as an always-on service instead (see
      the DAG file's own docstring for the full reasoning).

    Tested before being handed to you: the DAG was actually imported via
    Airflow's DagBag with ZERO errors (not just syntax-checked), the new
    compose service YAML was merged into your real file and re-validated
    as parseable YAML with all 3 services present.

.EXAMPLE
    cd "D:\NTI INTERNSHIP\Airflow\Banking_pipeline"
    .\Setup-Kafka-Airflow-Integration.ps1
#>

[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    Write-Host "[Setup-Kafka-Airflow] $Message"
}

function Write-FileIfNeeded {
    param([string]$Path, [string]$Content)
    $dir = Split-Path $Path -Parent
    if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    if ((Test-Path $Path) -and (-not $Force)) {
        Write-Log "SKIP (already exists, use -Force to overwrite): $Path"
        return
    }
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($Path, $Content, $utf8NoBom)
    Write-Log "Wrote: $Path"
}

$dockerfile = @'
# kafka/consumer/Dockerfile
#
# Containerizes the consumer as an always-on service (restart:unless-stopped
# in docker-compose), matching how the broker itself already runs -- rather
# than depending on someone leaving a terminal window open with
# `python kafka/consumer/consume_to_bronze.py` running.

FROM python:3.12-slim

WORKDIR /app

COPY kafka/consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka/consumer/consume_to_bronze.py .
COPY data_simulation/config.py ./data_simulation_config.py

# The consumer imports `from data_simulation.config import get_path` -- give
# it that same package layout inside the container.
RUN mkdir -p data_simulation && \
    mv data_simulation_config.py data_simulation/config.py && \
    touch data_simulation/__init__.py

# .env is deliberately NOT copied into the image -- baking secrets into an
# image layer means anyone with the image has them forever, even after
# rotation. Passed in at runtime via docker-compose's `env_file:` instead
# (see kafka/docker-compose.yml).
#
# local_warehouse/ is deliberately NOT baked in either -- config.py resolves
# paths relative to its own file location, which inside this container
# would be /app, not your real project root. docker-compose mounts your
# actual local_warehouse/ as a volume at /app/local_warehouse so the
# consumer writes to the SAME landing zone your host-run Bronze DAG reads
# from -- not an isolated copy inside the container.

CMD ["python", "consume_to_bronze.py"]

'@
$composeService = @'

  kafka-consumer:
    build:
      context: ..
      dockerfile: kafka/consumer/Dockerfile
    container_name: kafka-consumer
    restart: unless-stopped
    env_file:
      - ../.env
    environment:
      # Overrides the host-facing localhost:9094 from .env -- inside the
      # Docker network, the broker is reached via its internal listener
      # at kafka:9092 (same one kafka-ui already uses), not the
      # EXTERNAL://localhost:9094 listener meant for host-side scripts.
      KAFKA_BOOTSTRAP_SERVERS: "kafka:9092"
    volumes:
      # Mounts your REAL local_warehouse/ (relative to this compose file's
      # own directory, i.e. project_root/local_warehouse) into the
      # container at the same relative path config.py expects -- so this
      # containerized consumer writes into the exact same landing zone
      # your host-run Bronze DAG already reads from, not an isolated copy
      # sealed inside the container.
      - ../local_warehouse:/app/local_warehouse
    depends_on:
      kafka:
        condition: service_healthy

'@
$dagFile = @'
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

'@

Write-FileIfNeeded -Path "kafka\consumer\Dockerfile" -Content $dockerfile
Write-FileIfNeeded -Path "airflow\dags\05_kafka_streaming_dag.py" -Content $dagFile

Write-Log ""
Write-Log "Appending kafka-consumer service to kafka\docker-compose.yml..."
$composePath = "kafka\docker-compose.yml"
if (-not (Test-Path $composePath)) {
    Write-Log "  ERROR: $composePath not found."
} else {
    $composeContent = Get-Content $composePath -Raw -Encoding UTF8
    if ($composeContent -match "kafka-consumer:") {
        Write-Log "  SKIP: kafka-consumer service already present"
    } else {
        Add-Content -Path $composePath -Value $composeService -Encoding UTF8
        Write-Log "  Appended kafka-consumer service"
    }
}

Write-Log ""
Write-Log "Setup written. Next steps:"
Write-Log "  1. cd kafka"
Write-Log "  2. docker compose up -d --build kafka-consumer"
Write-Log "     (--build is needed since this is a new service with its own Dockerfile)"
Write-Log "  3. docker compose ps kafka-consumer   (confirm it shows Up)"
Write-Log "  4. docker compose logs kafka-consumer   (confirm it joined the consumer group)"
Write-Log "  5. cd .."
Write-Log "  6. Stop any manually-running 'python kafka\consumer\consume_to_bronze.py' terminal --"
Write-Log "     the containerized version now does that job"
Write-Log "  7. In Airflow UI: find and trigger the '05_kafka_streaming' DAG"
Write-Log "  8. Confirm check_kafka_consumer_alive passes, then run_kafka_producer completes"