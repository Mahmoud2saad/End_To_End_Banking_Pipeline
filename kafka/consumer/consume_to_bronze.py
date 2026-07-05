"""
kafka/consumer/consume_to_bronze.py

Consumes the domain-table topics produced by generate_events.py and writes
micro-batch Parquet files into the SAME landing-zone paths that
data_simulation/stream_simulator.py used to write to directly, and that
notebooks/local/01_bronze_local.py already knows how to ingest.

This is the key integration decision: Kafka does NOT introduce a new,
separate storage path (the old PoC wrote to data/bronze/transactions/
events.jsonl and a raw Postgres table, disconnected from the medallion
pipeline). Instead, Kafka becomes the mechanism that FILLS the existing
landing zone — Airflow's 01_bronze_ingestion DAG, dbt, and everything
downstream needs zero changes.

Design:
- Subscribes to all topics under KAFKA_TOPIC_PREFIX using pattern
  subscription (so adding a new domain table later doesn't require
  redeploying the consumer).
- Buffers messages per-topic in memory; flushes to a timestamped Parquet
  file per topic when either the batch size or the flush interval is hit
  (whichever comes first) — this bounds both memory use and latency.
- Uses pandas + pyarrow directly rather than spinning up a full Spark
  session per micro-batch, since this runs continuously and a JVM/Spark
  session per flush would be wasteful. Spark is still used downstream by
  01_bronze_local.py, which reads whatever Parquet files land here.
- Writes to a domain-specific landing path resolved via
  data_simulation.config.get_path("landing", domain) — same path
  resolution logic the rest of the pipeline already uses, so this respects
  the ENV (dev/staging/prod) split from docs/SECURITY_AND_GOVERNANCE.md.
- Commits Kafka offsets only AFTER a successful flush to disk — this is
  the mechanism that makes Kafka retention your Bronze recovery path (see
  docs/DR_RUNBOOK.md's "Bronze/Silver Delta tables — via Kafka replay"
  section): if the consumer crashes mid-batch, uncommitted messages get
  redelivered and re-flushed, not lost.
"""
import json
import logging
import os
import sys
import time
import uuid
from collections import defaultdict
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv(PROJECT_ROOT / ".env")

from data_simulation.config import get_path  # noqa: E402

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO"),
)
logger = logging.getLogger("kafka_consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "banking.dev")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "bronze-ingestion-consumer")

FLUSH_BATCH_SIZE = int(os.getenv("CONSUMER_FLUSH_BATCH_SIZE", 100))
FLUSH_INTERVAL_SECS = float(os.getenv("CONSUMER_FLUSH_INTERVAL_SECS", 10.0))

# Maps a Kafka topic's domain suffix -> the landing-zone table name that
# data_simulation.config.get_path("landing", <name>) expects. Mirrors the
# LANDING_* constants in config.py so this consumer never invents its own
# path convention.
DOMAIN_TO_LANDING_TABLE = {
    "atm_master": "atm_master",
    "customers": "customers",
    "cards": "cards",
    "card_transactions": "card_transactions",
    "wallet_transactions": "wallet_transactions",
    "out_of_cash": "out_of_cash",
    "kaggle_transactions": "kaggle_transactions",
}


def flush_buffer(domain: str, records: list[dict]) -> bool:
    """Writes a buffered batch of records to a timestamped Parquet file in
    this domain's landing path. Returns True on success."""
    if not records:
        return True

    landing_table = DOMAIN_TO_LANDING_TABLE.get(domain)
    if landing_table is None:
        logger.warning(f"[{domain}] no landing-table mapping defined — dropping {len(records)} records")
        return False

    landing_path = get_path("landing", landing_table)
    os.makedirs(landing_path, exist_ok=True)

    filename = f"{domain}_{pd.Timestamp.utcnow().strftime('%Y%m%dT%H%M%S%f')}_{uuid.uuid4().hex[:8]}.parquet"
    filepath = os.path.join(landing_path, filename)

    try:
        df = pd.DataFrame(records)
        df["_kafka_consumed_at"] = pd.Timestamp.utcnow().isoformat()
        df.to_parquet(filepath, engine="pyarrow", compression="snappy", index=False)
        logger.info(f"[{domain}] flushed {len(records)} records -> {filepath}")
        return True
    except Exception:
        logger.exception(f"[{domain}] flush FAILED for {len(records)} records — will not commit offsets for this batch")
        return False


def main():
    logger.info("=" * 70)
    logger.info("Kafka consumer starting")
    logger.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic prefix (pattern): ^{KAFKA_TOPIC_PREFIX}\\..*")
    logger.info(f"Consumer group: {CONSUMER_GROUP}")
    logger.info(f"Flush batch size: {FLUSH_BATCH_SIZE}, flush interval: {FLUSH_INTERVAL_SECS}s")
    logger.info("=" * 70)

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,  # manual commit AFTER successful flush — see module docstring
    )
    # Pattern subscription: any topic matching "<prefix>.<anything>"
    import re
    consumer.subscribe(pattern=re.compile(rf"^{re.escape(KAFKA_TOPIC_PREFIX)}\..*"))

    buffers: dict[str, list[dict]] = defaultdict(list)
    pending_offsets: dict[str, list] = defaultdict(list)
    last_flush_time = time.time()

    def flush_all(reason: str):
        nonlocal last_flush_time
        for domain, records in list(buffers.items()):
            if not records:
                continue
            success = flush_buffer(domain, records)
            if success:
                consumer.commit()
                buffers[domain] = []
                pending_offsets[domain] = []
            else:
                logger.error(f"[{domain}] leaving {len(records)} records uncommitted for redelivery")
        last_flush_time = time.time()
        logger.debug(f"flush_all triggered by: {reason}")

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000, max_records=200)

            for tp, messages in msg_pack.items():
                for msg in messages:
                    domain = tp.topic.split(f"{KAFKA_TOPIC_PREFIX}.", 1)[-1]
                    buffers[domain].append(msg.value)

                    if len(buffers[domain]) >= FLUSH_BATCH_SIZE:
                        success = flush_buffer(domain, buffers[domain])
                        if success:
                            consumer.commit()
                            buffers[domain] = []
                        else:
                            logger.error(f"[{domain}] batch-size flush failed — records stay buffered for retry")

            if time.time() - last_flush_time >= FLUSH_INTERVAL_SECS:
                flush_all(reason="interval elapsed")

    except KeyboardInterrupt:
        logger.info("Interrupted — flushing remaining buffers before exit...")
        flush_all(reason="shutdown")
    finally:
        consumer.close()
        logger.info("Consumer shut down cleanly.")


if __name__ == "__main__":
    main()
