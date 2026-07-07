"""
grafana_api/observability.py

Adds three endpoints to the existing grafana_api FastAPI app (see
main.py's existing /pipeline-health, which already works and is untouched
by this file): Kafka consumer lag, backup freshness, and Bronze/Silver
landing freshness. See docs/SLOS.md for the targets these measure against.

Wired into main.py with:
    from grafana_api.observability import router as observability_router
    app.include_router(observability_router)

(a two-line addition to the existing file, not a rewrite of it.)
"""
import os
import glob
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException

router = APIRouter()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC_PREFIX = os.environ.get("KAFKA_TOPIC_PREFIX", "banking.dev")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "bronze-ingestion-consumer")
KAFKA_LAG_SLO = int(os.environ.get("KAFKA_LAG_SLO", 500))

BACKUP_DEST = os.environ.get("BACKUP_DEST", "./backups")
BACKUP_SLO_HOURS = float(os.environ.get("BACKUP_SLO_HOURS", 26))

LANDING_PATH = os.environ.get(
    "LANDING_PATH",
    os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                 "local_warehouse", "delta", "landing"),
)
FRESHNESS_SLO_MINUTES = float(os.environ.get("FRESHNESS_SLO_MINUTES", 15))


# ── Kafka consumer lag ────────────────────────────────────────────────────────
@router.get("/kafka-lag")
def kafka_lag():
    """
    Per-topic consumer lag for KAFKA_CONSUMER_GROUP: (log end offset) -
    (last committed offset), summed per topic across all partitions.

    NOTE: requires a reachable Kafka broker -- if KAFKA_BOOTSTRAP_SERVERS
    isn't reachable (e.g. the Kafka docker-compose stack isn't running),
    this returns a 503 with the connection error rather than crashing the
    whole API, since Grafana should be able to show "Kafka unreachable"
    as a status rather than losing every other panel too.
    """
    try:
        from kafka import KafkaConsumer, TopicPartition
        from kafka.admin import KafkaAdminClient
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail="kafka-python not installed in this environment. "
                   "pip install kafka-python (see kafka/consumer/requirements.txt)",
        )

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_CONSUMER_GROUP,
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
        )
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

        all_topics = consumer.topics()
        our_topics = [t for t in all_topics if t.startswith(f"{KAFKA_TOPIC_PREFIX}.")]

        if not our_topics:
            consumer.close()
            admin.close()
            return {
                "consumer_group": KAFKA_CONSUMER_GROUP,
                "topics": [],
                "total_lag": 0,
                "note": f"No topics found matching prefix '{KAFKA_TOPIC_PREFIX}.' "
                        f"-- has the producer been run yet?",
            }

        partitions = []
        for topic in our_topics:
            for p in consumer.partitions_for_topic(topic) or []:
                partitions.append(TopicPartition(topic, p))

        consumer.assign(partitions)
        end_offsets = consumer.end_offsets(partitions)

        # committed() returns None for a partition the group has never
        # committed on -- treat that as lag == full end offset (nothing
        # consumed yet), not as an error.
        results_by_topic = {}
        total_lag = 0
        for tp in partitions:
            end = end_offsets.get(tp, 0)
            committed = consumer.committed(tp)
            committed = committed if committed is not None else 0
            lag = max(end - committed, 0)
            total_lag += lag

            entry = results_by_topic.setdefault(tp.topic, {
                "topic": tp.topic, "end_offset": 0, "committed_offset": 0, "lag": 0,
            })
            entry["end_offset"] += end
            entry["committed_offset"] += committed
            entry["lag"] += lag

        consumer.close()
        admin.close()

        topics_out = list(results_by_topic.values())
        for t in topics_out:
            t["within_slo"] = t["lag"] <= KAFKA_LAG_SLO

        return {
            "consumer_group": KAFKA_CONSUMER_GROUP,
            "slo_threshold": KAFKA_LAG_SLO,
            "topics": sorted(topics_out, key=lambda t: -t["lag"]),
            "total_lag": total_lag,
            "within_slo": total_lag <= KAFKA_LAG_SLO * max(len(topics_out), 1),
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Could not reach Kafka at {KAFKA_BOOTSTRAP_SERVERS}: {e}",
        )


# ── Backup freshness ──────────────────────────────────────────────────────────
def _latest_file_age_hours(pattern: str) -> tuple[str | None, float | None]:
    """Returns (filename, age_in_hours) for the most recently modified file
    matching pattern, or (None, None) if nothing matches."""
    files = glob.glob(pattern)
    if not files:
        return None, None
    latest = max(files, key=os.path.getmtime)
    age_seconds = time.time() - os.path.getmtime(latest)
    return os.path.basename(latest), round(age_seconds / 3600, 2)


@router.get("/backup-health")
def backup_health():
    """
    Reports age of the most recent DuckDB and Postgres backup, checked
    against BACKUP_SLO_HOURS (default 26h -- see docs/SLOS.md for why).
    """
    duckdb_file, duckdb_age = _latest_file_age_hours(
        os.path.join(BACKUP_DEST, "duckdb", "*.duckdb")
    )
    postgres_file, postgres_age = _latest_file_age_hours(
        os.path.join(BACKUP_DEST, "postgres", "*.sql.gz")
    )

    def status_for(age):
        if age is None:
            return "NO_BACKUP_FOUND"
        return "OK" if age <= BACKUP_SLO_HOURS else "STALE"

    return {
        "slo_threshold_hours": BACKUP_SLO_HOURS,
        "duckdb": {
            "latest_file": duckdb_file,
            "age_hours": duckdb_age,
            "status": status_for(duckdb_age),
        },
        "postgres": {
            "latest_file": postgres_file,
            "age_hours": postgres_age,
            "status": status_for(postgres_age),
        },
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }


# ── Bronze/Silver landing freshness ───────────────────────────────────────────
@router.get("/freshness")
def freshness():
    """
    Age of the most recent landing-zone file per domain table -- see
    docs/SLOS.md's caveat about this being meaningful only while the
    producer/consumer are actively running, not as an always-on service.
    """
    if not os.path.isdir(LANDING_PATH):
        return {"error": f"Landing path not found: {LANDING_PATH}"}

    domains = [
        d for d in os.listdir(LANDING_PATH)
        if os.path.isdir(os.path.join(LANDING_PATH, d))
    ]

    results = []
    for domain in sorted(domains):
        pattern = os.path.join(LANDING_PATH, domain, "*.parquet")
        latest_file, age_hours = _latest_file_age_hours(pattern)
        age_minutes = round(age_hours * 60, 1) if age_hours is not None else None
        results.append({
            "domain": domain,
            "latest_file": latest_file,
            "age_minutes": age_minutes,
            "within_slo": (age_minutes is not None and age_minutes <= FRESHNESS_SLO_MINUTES),
        })

    return {
        "slo_threshold_minutes": FRESHNESS_SLO_MINUTES,
        "domains": results,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
