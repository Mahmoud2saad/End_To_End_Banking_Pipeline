"""
kafka/producer/generate_events.py

Streams REAL banking domain data (not generic fake transactions) into Kafka,
one topic per domain table. This replaces the earlier PoC producer, which
generated synthetic branch/account events unrelated to the actual pipeline.

Design:
- Reads the same source CSVs data_simulation/config.py already knows about
  (single source of truth for paths — no path duplicated here).
- Applies PAN/card-number tokenization at the producer boundary (never lets
  a raw PAN reach a topic) — see scripts/pan_tokenize.py and
  docs/SECURITY_AND_GOVERNANCE.md section 1.
- Drops CVV entirely — this is never persisted past the source file, even
  in synthetic data, to demonstrate the correct real-world pattern.
- Honors SIMULATOR_MODE / SIMULATOR_LIMIT_ROWS / SIMULATOR_BATCH_SIZE /
  SIMULATOR_DELAY_SECS from .env, because the real datasets are large
  (cards.csv ~4.06M rows, kaggle transactions ~13.3M rows) — at naive
  defaults a full replay would take many hours. demo mode caps rows per
  table so this is actually runnable for a screen-recording/interview demo.
- One producer thread per domain table, all publishing concurrently, so the
  topics fill the way real concurrent source systems would (not one table
  finishing before the next starts).
- Deterministic partition key per table (e.g. PAN token, client_id, terminal
  id) so records for the same entity land on the same partition — this
  matters for consumer-side ordering guarantees per entity.

Each topic is named f"{KAFKA_TOPIC_PREFIX}.{domain}", e.g.
"banking.dev.card_transactions". KAFKA_TOPIC_PREFIX comes from .env and
should differ per environment (banking.dev / banking.staging / banking.prod)
per docs/SECURITY_AND_GOVERNANCE.md section 4.

NOTE — known gap, not addressed by this producer: there is no standalone
atm_transactions source file in data/synthetic/, and the existing
notebooks/local/01_bronze_local.py never ingests LANDING_ATM_TRANSACTIONS
either. ATM transaction data currently only exists implicitly (e.g. within
out_of_cash.csv's terminal-level records). This producer intentionally does
NOT invent a fake atm_transactions source to paper over that gap — it's a
pre-existing modeling gap that should be resolved explicitly (see repo
follow-up), not silently patched here.
"""
import json
import logging
import os
import sys
import threading
import time
import uuid
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer

# ── Path setup: reuse the project's single source of truth for paths ────────
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

load_dotenv(PROJECT_ROOT / ".env")

from data_simulation.config import (  # noqa: E402
    ATM_MASTER_FILE, CARDS_TXN_FILE, WALLET_FILE, OUT_OF_CASH_FILE,
    USERS_FILE, CARDS_FILE, KAGGLE_TRANSACTIONS,
)
from pan_tokenize import tokenize_pan  # noqa: E402

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s",
    level=os.getenv("LOG_LEVEL", "INFO"),
)
logger = logging.getLogger("kafka_producer")

# ── Config from .env ──────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "banking.dev")

SIMULATOR_MODE = os.getenv("SIMULATOR_MODE", "demo")  # demo | realistic | full
SIMULATOR_BATCH_SIZE = int(os.getenv("SIMULATOR_BATCH_SIZE", 50))
SIMULATOR_DELAY_SECS = float(os.getenv("SIMULATOR_DELAY_SECS", 5.0))
SIMULATOR_LIMIT_ROWS = os.getenv("SIMULATOR_LIMIT_ROWS")
SIMULATOR_LIMIT_ROWS = int(SIMULATOR_LIMIT_ROWS) if SIMULATOR_LIMIT_ROWS else None

# demo mode always caps rows even if SIMULATOR_LIMIT_ROWS isn't set, so a
# forgotten .env value can't accidentally trigger a 13M-row replay.
if SIMULATOR_MODE == "demo" and SIMULATOR_LIMIT_ROWS is None:
    SIMULATOR_LIMIT_ROWS = 2000
if SIMULATOR_MODE == "full":
    SIMULATOR_LIMIT_ROWS = None  # explicit opt-in to a full replay

# realistic mode: no row cap, but a much shorter delay than the old 5s
# default so a full run finishes in a reasonable showcase window rather
# than the ~112 hours a naive full cards.csv replay would take at 50/5s.
EFFECTIVE_DELAY = SIMULATOR_DELAY_SECS if SIMULATOR_MODE == "demo" else min(SIMULATOR_DELAY_SECS, 0.5)


def topic(domain: str) -> str:
    return f"{KAFKA_TOPIC_PREFIX}.{domain}"


_producer_lock = threading.Lock()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
        acks="all",
        retries=5,
        linger_ms=50,
    )


def stream_table(
    producer: KafkaProducer,
    source_file: str,
    domain: str,
    key_column: str,
    pan_columns: list[str] | None = None,
    drop_columns: list[str] | None = None,
):
    """Reads source_file in chunks, tokenizes PAN columns, drops sensitive
    columns that should never leave the source system (e.g. CVV), and
    publishes each row to Kafka."""
    pan_columns = pan_columns or []
    drop_columns = drop_columns or []
    t = topic(domain)

    if not os.path.exists(source_file):
        logger.warning(f"[{domain}] source file not found, skipping: {source_file}")
        return

    rows_sent = 0
    try:
        for chunk in pd.read_csv(source_file, chunksize=SIMULATOR_BATCH_SIZE, low_memory=False):
            if SIMULATOR_LIMIT_ROWS is not None and rows_sent >= SIMULATOR_LIMIT_ROWS:
                break

            for col in pan_columns:
                if col in chunk.columns:
                    chunk[col] = chunk[col].apply(
                        lambda v: tokenize_pan(v) if pd.notna(v) else None
                    )
            for col in drop_columns:
                if col in chunk.columns:
                    chunk = chunk.drop(columns=[col])

            chunk.columns = [str(c).strip().lower().replace(" ", "_") for c in chunk.columns]

            for _, row in chunk.iterrows():
                if SIMULATOR_LIMIT_ROWS is not None and rows_sent >= SIMULATOR_LIMIT_ROWS:
                    break
                payload = row.where(pd.notna(row), None).to_dict()
                payload["_event_id"] = str(uuid.uuid4())
                payload["_produced_at"] = pd.Timestamp.utcnow().isoformat()
                payload["_source_domain"] = domain

                key_col_normalized = key_column.strip().lower().replace(" ", "_")
                key = payload.get(key_col_normalized)

                with _producer_lock:
                    producer.send(t, key=key, value=payload)

                rows_sent += 1

            logger.info(f"[{domain}] sent {rows_sent:,} rows so far (topic={t})")
            time.sleep(EFFECTIVE_DELAY)

    except Exception:
        logger.exception(f"[{domain}] streaming failed after {rows_sent:,} rows")
        raise
    finally:
        logger.info(f"[{domain}] done — {rows_sent:,} total rows sent to {t}")


# ── Domain table definitions ─────────────────────────────────────────────────
# (source_file, domain_name, partition_key_column, pan_columns_to_tokenize, columns_to_drop)
TABLES = [
    (ATM_MASTER_FILE,     "atm_master",         "Terminal ID",  [],       []),
    (USERS_FILE,          "customers",          "id",           [],       []),
    (CARDS_FILE,          "cards",              "id",           ["card_number"], ["cvv"]),
    (CARDS_TXN_FILE,      "card_transactions",  "PAN",          ["PAN"],  []),
    (WALLET_FILE,         "wallet_transactions","Mobile Number",[],       []),
    (OUT_OF_CASH_FILE,    "out_of_cash",        "PAN",          ["PAN"],  []),
    (KAGGLE_TRANSACTIONS, "kaggle_transactions","client_id",    [],       []),
]


def main():
    logger.info("=" * 70)
    logger.info(f"Kafka producer starting — mode={SIMULATOR_MODE}, "
                f"limit_rows_per_table={SIMULATOR_LIMIT_ROWS}, "
                f"batch_size={SIMULATOR_BATCH_SIZE}, delay={EFFECTIVE_DELAY}s")
    logger.info(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic prefix: {KAFKA_TOPIC_PREFIX}")
    logger.info("=" * 70)

    producer = make_producer()
    threads = []

    for source_file, domain, key_col, pan_cols, drop_cols in TABLES:
        th = threading.Thread(
            target=stream_table,
            name=f"producer-{domain}",
            args=(producer, source_file, domain, key_col, pan_cols, drop_cols),
            daemon=True,
        )
        threads.append(th)
        th.start()

    try:
        for th in threads:
            th.join()
    except KeyboardInterrupt:
        logger.info("Interrupted — flushing producer before exit...")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer shut down cleanly.")


if __name__ == "__main__":
    main()
