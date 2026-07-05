# Kafka Streaming Layer

Real, working Kafka streaming for the Banking Pipeline — replaces the
earlier PoC, which generated generic fake transactions disconnected from
the actual domain model.

## What changed from the original PoC

The original `generate_events.py`/`consume_to_bronze.py` generated random
`branch_code`/`acc_no`/`trans_desc` events unrelated to the real synthetic
datasets, and the consumer wrote to a standalone `data/bronze/transactions/
events.jsonl` file plus a separate raw Postgres table
(`load_jsonl_to_postgres.py`) — a third landing spot, disconnected from the
medallion pipeline (DuckDB/Delta/dbt) entirely.

**This version streams your actual domain data** — the same
`data/synthetic/*.csv` and `data/kaggle/transactions_data.csv` files the
rest of the pipeline already uses — and writes output into the **exact same
landing-zone paths** that `notebooks/local/01_bronze_local.py` already
knows how to ingest. Kafka is now the mechanism that *fills* the landing
zone; nothing downstream (Airflow's `01_bronze_ingestion` DAG, dbt) needs
to change.

`kafka/load_jsonl_to_postgres.py` is now unused by this flow — recommend
deleting it in your next cleanup pass, since keeping two ingestion paths
around invites confusion about which one is "real."

## Architecture

```
data/synthetic/*.csv, data/kaggle/*.csv
        │
        ▼  (tokenize PAN, drop CVV, chunked read)
kafka/producer/generate_events.py  ──publishes──▶  Kafka topics
                                                    banking.dev.atm_master
                                                    banking.dev.customers
                                                    banking.dev.cards
                                                    banking.dev.card_transactions
                                                    banking.dev.wallet_transactions
                                                    banking.dev.out_of_cash
                                                    banking.dev.kaggle_transactions
                                                            │
                                                            ▼ (pattern subscription)
                                      kafka/consumer/consume_to_bronze.py
                                                            │
                                                            ▼ (micro-batch Parquet)
                                      local_warehouse/delta/landing/<table>/
                                                            │
                                                            ▼ (unchanged — existing DAG)
                                      notebooks/local/01_bronze_local.py
                                      (Airflow: 01_bronze_ingestion DAG)
                                                            │
                                                            ▼
                                      local_warehouse/delta/bronze/<table>/
```

## Known gap (not fixed by this rebuild)

There is no standalone `atm_transactions.csv` source file, and
`01_bronze_local.py` never ingests `LANDING_ATM_TRANSACTIONS` even though
`config.py` defines it and `stg_atm_transactions`/`fact_atm_transactions`
exist downstream in dbt. This producer does not stream a fake
`atm_transactions` topic to paper over that — it's a pre-existing modeling
gap that needs an explicit decision (derive ATM transactions from
`out_of_cash.csv`'s terminal-level records? add a real source file?) rather
than a silent workaround here.

## Running it

1. Start the Kafka broker + UI:
   ```
   cd kafka
   docker compose up -d
   ```
   Kafka UI at http://localhost:8080 to watch topics/messages live.

2. Install dependencies (from the project root, with your venv active):
   ```
   pip install -r kafka/producer/requirements.txt
   pip install -r kafka/consumer/requirements.txt
   ```

3. Start the consumer first (so it's ready before the producer sends):
   ```
   python kafka/consumer/consume_to_bronze.py
   ```

4. In a separate terminal, start the producer:
   ```
   python kafka/producer/generate_events.py
   ```

## Throughput modes

Controlled via `.env` (`SIMULATOR_MODE`), because the real datasets are
large — `cards.csv` alone is ~4.06M rows, the Kaggle transactions file is
~13.3M rows:

- **`demo`** (default): caps each table at `SIMULATOR_LIMIT_ROWS` (2000 by
  default) — finishes in seconds, safe for a live demo or screen recording.
- **`realistic`**: no row cap, shorter inter-batch delay — a genuine
  longer-running showcase, not a full replay.
- **`full`**: no cap at all — a true full replay of every row. At full
  volume this takes a long time; only use this deliberately, with time to
  spare, not as a default.

## Security

- PAN and `card_number` are tokenized (HMAC-SHA256, see
  `scripts/pan_tokenize.py`) **before** anything reaches a Kafka topic —
  never in plaintext past the producer.
- `cvv` is dropped entirely, never published, matching real-world PCI-DSS
  practice even though this is synthetic data.
- Topic names are environment-scoped via `KAFKA_TOPIC_PREFIX`
  (`banking.dev.*` / `banking.staging.*` / `banking.prod.*`) — see
  `docs/SECURITY_AND_GOVERNANCE.md` section 4.
- Current `kafka/docker-compose.yml` runs `PLAINTEXT`, single broker — fine
  for local dev, not for staging/prod. `SASL_SSL` config is already
  templated in the root `.env.example`'s Kafka section for when this moves
  beyond a laptop.

## Recovery

Kafka's own retention window (`KAFKA_RETENTION_HOURS` in `.env`) is your
Bronze-layer recovery mechanism — see `docs/DR_RUNBOOK.md`'s "Bronze/Silver
Delta tables — via Kafka replay" section for the exact consumer-group
offset-reset procedure.
