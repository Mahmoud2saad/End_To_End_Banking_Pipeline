# 🏦 End-to-End Banking Pipeline

> A production-grade data engineering platform built for banking analytics. Runs fully **local** (DuckDB + PySpark + Delta Lake) or fully **cloud** (Azure Data Lake + Databricks), with real-time streaming via **Apache Kafka**, Gold-layer transformation via **dbt**, orchestration via **Apache Airflow**, a custom **FastAPI observability layer**, and monitoring via **Grafana**.

---

## 📌 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Data Model](#data-model)
- [Medallion Layers](#medallion-layers)
- [Airflow Orchestration](#airflow-orchestration)
- [Kafka Streaming](#kafka-streaming)
- [Grafana API & Observability](#grafana-api--observability)
- [CI/CD](#cicd)
- [Cloud Deployment](#cloud-deployment-azure--databricks)
- [Local Setup](#local-setup-docker--airflow)
- [Environment Variables](#environment-variables)
- [DAG Reference](#dag-reference)
- [SLOs](#slos)
- [Troubleshooting](#troubleshooting)
- [Contributors](#contributors)

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         BANKING PIPELINE — HYBRID ARCHITECTURE                   │
│                                                                                  │
│  ┌──────────────┐    ┌───────────────────────────────────────────────────────┐   │
│  │  CSV Sources │───▶│              APACHE KAFKA (Real-time)                 │   │
│  │  / Simulator │    │  ATM · Cards · Customers · Wallets · Kaggle Txns      │   │
│  └──────────────┘    └────────────────────┬──────────────────────────────────┘   │
│                                           │ Consumer → Landing Zone              │
│           ┌───────────────────────────────▼──────────────────────────────────┐   │
│           │              LANDING ZONE  (Parquet, partitioned by date)        │   │
│           └───────────────────────────────┬──────────────────────────────────┘   │
│                                           │                                      │
│           ┌───────────────────────────────▼──────────────────────────────────┐   │
│           │               BRONZE LAYER  (Raw Delta / Parquet)                │   │
│           │         Schema enforcement · Append-only · Full audit trail      │   │
│           └───────────────────────────────┬──────────────────────────────────┘   │
│                                           │ PySpark / Databricks                 │
│           ┌───────────────────────────────▼──────────────────────────────────┐   │
│           │               SILVER LAYER  (Cleaned Delta Tables)               │   │
│           │   Deduplication · Type casting · Null handling · SCD tracking    │   │
│           └───────────────────────────────┬──────────────────────────────────┘   │
│                                           │ dbt (DuckDB / Databricks)            │
│           ┌───────────────────────────────▼──────────────────────────────────┐   │
│           │               GOLD LAYER  (Dimensional Model / Marts)            │   │
│           │  dim_* · fact_* · ATM Performance · Fraud Risk · Spending KPIs   │   │
│           └───────────────────────────────┬──────────────────────────────────┘   │
│                                           │                                      │
│  ┌─────────────────┐   ┌─────────────────▼──────────┐   ┌───────────────────┐   │
│  │ FastAPI          │   │   DuckDB / Snowflake        │   │ Grafana Dashboards│   │
│  │ Observability   │   │   (Analytical Warehouse)    │   │ (via Infinity DS) │   │
│  │ /backup-health  │   └────────────────────────────┘    └───────────────────┘   │
│  │ /freshness      │                                                              │
│  │ /kafka-lag      │                                                              │
│  │ /pipeline-health│                                                              │
│  └─────────────────┘                                                              │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Key Features

| Feature | Details |
|---|---|
| **Hybrid execution** | Toggle `ENV=local` (DuckDB + local Spark) or `ENV=cloud` (Azure ADLS + Databricks) — same DAGs, same dbt models |
| **Medallion architecture** | Landing → Bronze → Silver → Gold with strict layer contracts |
| **Real-time streaming** | Apache Kafka ingests card transactions, ATM events, customers, wallets from real CSV sources |
| **PAN tokenization** | Card numbers are HMAC-SHA256 tokenized at the producer; CVV is never published |
| **dbt Gold layer** | 7 dimensions, 4 fact tables, 6 analytics marts — all tested with dbt's built-in quality suite |
| **Orchestration** | Apache Airflow 2.8.1 with ExternalTaskSensor chaining, email alerting, and custom operators |
| **FastAPI observability** | `/backup-health`, `/freshness`, `/kafka-lag`, `/pipeline-health` endpoints with SLO tracking |
| **Grafana monitoring** | 15+ API endpoints feed live Grafana dashboards via the Infinity datasource |
| **CI/CD** | GitHub Actions: dbt compile + test on every push; dbt docs auto-deployed to GitHub Pages |
| **Great Expectations** | Data quality checks at the Silver layer |
| **Dockerised Airflow** | One-command local stack: Postgres + Airflow webserver + scheduler |

---

## Project Structure

```
Banking_Pipeline/
├── .github/
│   └── workflows/
│       ├── dbt_ci.yml               # CI: dbt compile + test on push to banking_dbt/
│       └── dbt_docs_cd.yml          # CD: deploy dbt docs to GitHub Pages
│
├── airflow/
│   ├── dags/
│   │   ├── 00_generate_data_dag.py  # Manual: stream simulator → landing zone
│   │   ├── 01_bronze_dag.py         # Scheduled: landing → Bronze (every 30 min)
│   │   ├── 02_silver_dag.py         # Scheduled: Bronze → Silver (offset 15 min)
│   │   ├── 03_dbt_dag.py            # Scheduled: Silver → Gold via dbt (hourly)
│   │   ├── 04_full_pipeline_dag.py  # Single DAG: full E2E run
│   │   └── 05_kafka_streaming_dag.py # Kafka producer/consumer lifecycle
│   └── plugins/operators/
│       ├── spark_operator.py        # SparkSubmitLocalOperator
│       └── dbt_operator.py          # DbtOperator wrapping dbt CLI
│
├── banking_dbt/                     # dbt project
│   ├── models/
│   │   ├── staging/                 # Cleaned Silver views
│   │   └── marts/
│   │       ├── dimensions/          # dim_customer, dim_card, dim_atm, dim_date …
│   │       ├── facts/               # fact_card_transactions, fact_atm …
│   │       └── gold/                # Analytics marts: fraud_risk, atm_performance …
│   ├── snapshots/                   # SCD Type 2: customers_snapshot, cards_snapshot
│   ├── seeds/                       # dim_channel.csv, dim_error_type.csv
│   └── tests/                       # Custom dbt data quality tests
│
├── data_simulation/
│   ├── stream_simulator.py          # Faker-based Parquet generator (landing zone)
│   ├── config.py                    # Path and domain config
│   └── upload_to_adls.py            # Cloud upload helper
│
├── grafana_api/
│   ├── main.py                      # FastAPI app: 15+ Grafana endpoints
│   ├── observability.py             # /backup-health, /freshness, /kafka-lag
│   └── requirements.txt
│
├── grafana_dashboards/              # Pre-built Grafana dashboard JSON exports
│
├── kafka/
│   ├── producer/generate_events.py  # Streams real CSV data to Kafka topics
│   ├── consumer/consume_to_bronze.py # Writes Kafka messages to landing zone Parquet
│   └── docker-compose.yml           # Kafka broker + Kafka UI
│
├── notebooks/
│   ├── local/                       # PySpark: landing → Bronze → Silver (local)
│   └── cloud/                       # Databricks: ADLS → Bronze → Silver
│
├── docs/
│   ├── SLOS.md                      # Service Level Objectives
│   ├── DR_RUNBOOK.md                # Disaster recovery procedures
│   ├── SECURITY_AND_GOVERNANCE.md   # PCI-DSS practices, topic scoping, webhook design
│   └── GRAFANA_OBSERVABILITY_SETUP.md
│
├── tests/dags/                      # DAG unit tests
├── scripts/                         # Utility scripts (PAN tokenization etc.)
├── Images/                          # Architecture and dashboard screenshots
├── docker-compose.yml               # Airflow stack (webserver + scheduler + postgres)
├── Dockerfile                       # Airflow 2.8.1 + Java 17 + PySpark + dbt
├── requirements.txt                 # All Python dependencies
├── requirements-dev.txt             # Dev/test dependencies
├── Makefile                         # Convenience targets
└── .env.example                     # Template for secrets
```

---

## Tech Stack

| Layer | Local | Cloud |
|---|---|---|
| Storage | Delta Lake (local FS) | Azure Data Lake Storage Gen2 |
| Processing | PySpark 3.4 (`local[*]`) | Azure Databricks |
| Warehouse | DuckDB 1.5 | Snowflake |
| Transformation | dbt-duckdb 1.7 | dbt-databricks 1.7 |
| Streaming | Apache Kafka (Docker) | Azure Event Hubs (Kafka-compatible) |
| Orchestration | Apache Airflow 2.8.1 | Apache Airflow 2.8.1 |
| Observability | FastAPI + custom endpoints | FastAPI + custom endpoints |
| Monitoring | Grafana + Infinity datasource | Grafana |
| Data Quality | Great Expectations 0.18 | Great Expectations 0.18 |
| CI | GitHub Actions (dbt compile + test) | GitHub Actions |
| CD | GitHub Pages (dbt docs) | GitHub Pages |

---

## Data Model

### Source Domains

| Domain | Tables | Description |
|---|---|---|
| **ATM Operations** | `atm_master`, `out_of_cash` | ATM locations, cash status, replenishment events |
| **Customers** | `customers`, `pan_customer_map` | Customer demographics, card-to-customer mapping |
| **Cards** | `cards`, `card_transactions` | Card metadata, all card-present and card-not-present transactions |
| **Wallets** | `wallet_transactions` | Mobile wallet transfers and top-ups |
| **Kaggle Reference** | `kaggle_transactions` | External fraud-labeled transaction dataset |

### Gold Layer (Dimensional Model)

**Dimensions:** `dim_customer` (SCD2) · `dim_card` · `dim_atm` · `dim_date` · `dim_geography` · `dim_merchant` · `dim_merchant_category`

**Facts:** `fact_card_transactions` · `fact_atm_transactions` · `fact_wallet_transactions` · `fact_out_of_cash_events`

**Analytics Marts:**
- `atm_performance` — utilisation rates, cash cycle times, failure rates, performance grade (A–D) per ATM per day
- `fraud_risk_scoring` — rule-based fraud score (0–100) per customer: dark web cards, fraud rate, behavioral signals
- `customer_spending_behavior` — RFM scoring, channel preferences, spend-to-income ratio, activity level
- `replenishment_analysis` — cash utilisation %, urgency (CRITICAL / HIGH / MEDIUM / LOW), OOC events per ATM
- `channel_comparison` — ATM vs card vs wallet volume and success rate by hour and time-of-day
- `governorate_summary` — regional transaction heatmap for operations dashboards

---

## Medallion Layers

**Landing Zone** — Raw Parquet files from the Kafka consumer or stream simulator, partitioned by `ingestion_date`. No schema enforcement; append-only.

**Bronze Layer** — PySpark reads landing zone, enforces schema, adds audit columns (`_source_file`, `_ingested_at`, `_pipeline_run_id`), writes immutable Delta tables. No deduplication.

**Silver Layer** — PySpark deduplicates on natural keys, applies type casting, null handling, standardised column naming, and SCD Type 2 tracking markers.

**Gold Layer** — dbt runs `seed → snapshot → staging → marts → test`. All models materialised as tables in DuckDB locally. dbt tests cover: not-null, unique, referential integrity, accepted values, and custom assertions.

---

## Airflow Orchestration

### DAG Chain

```
00_generate_data        (manual trigger)
        │
        ▼
01_bronze_ingestion     (*/30 * * * *)
        │ ExternalTaskSensor
        ▼
02_silver_transformation (15,45 * * * *)
        │ ExternalTaskSensor
        ▼
03_dbt_gold             (30 * * * *)
```

`04_full_pipeline` runs the full chain in a single DAG for dev/testing.
`05_kafka_streaming` manages Kafka producer and consumer lifecycle.

### Custom Operators
- `SparkSubmitLocalOperator` — runs PySpark scripts as subprocesses with `JAVA_HOME` and `PYTHONPATH` injection
- `DbtOperator` — wraps `dbt run / test / seed / snapshot` with profiles and project dir overrides

### Alerting
Every DAG sends email alerts on failure via Outlook SMTP. Success notifications include table counts and run metadata.

---

## Kafka Streaming

The Kafka layer streams **real domain data** from the same CSV files the rest of the pipeline uses, writing output into the exact landing-zone paths that `01_bronze_local.py` already ingests. Nothing downstream changes.

### Topics

| Topic | Source |
|---|---|
| `banking.dev.atm_master` | `data/synthetic/atm_master.csv` |
| `banking.dev.customers` | `data/synthetic/customers.csv` |
| `banking.dev.cards` | `data/synthetic/cards.csv` |
| `banking.dev.card_transactions` | `data/synthetic/card_transactions.csv` |
| `banking.dev.wallet_transactions` | `data/synthetic/wallet_transactions.csv` |
| `banking.dev.out_of_cash` | `data/synthetic/out_of_cash.csv` |
| `banking.dev.kaggle_transactions` | `data/kaggle/transactions_data.csv` |

### Throughput Modes (via `SIMULATOR_MODE` in `.env`)
- `demo` — caps each table at `SIMULATOR_LIMIT_ROWS` (default 2000). Finishes in seconds.
- `realistic` — no row cap, shorter inter-batch delay.
- `full` — complete replay of all rows. Use deliberately; large datasets take time.

### Security
- PAN and `card_number` are HMAC-SHA256 tokenized **before** reaching any Kafka topic
- `cvv` is dropped entirely, never published
- Topic names are environment-scoped via `KAFKA_TOPIC_PREFIX` (`banking.dev.*` / `banking.staging.*` / `banking.prod.*`)

### Running Kafka

```bash
cd kafka
docker compose up -d          # starts broker + Kafka UI at http://localhost:8080

# In terminal 1 (consumer first):
python kafka/consumer/consume_to_bronze.py

# In terminal 2 (producer):
python kafka/producer/generate_events.py
```

---

## Grafana API & Observability

A FastAPI service bridges Grafana and DuckDB, serving the Gold layer to Grafana dashboards via the Infinity datasource.

### Start the API

```bash
python -m uvicorn grafana_api.main:app --host 0.0.0.0 --port 8000 --reload
```

### Business Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Health check |
| `GET /dwh-info` | Available tables in DuckDB |
| `GET /atm-performance` | Daily ATM metrics per terminal |
| `GET /atm-performance/by-region` | ATM metrics aggregated by governorate |
| `GET /atm-performance/by-grade` | ATM count by performance grade A–D |
| `GET /replenishment` | Cash replenishment urgency per ATM |
| `GET /replenishment/by-urgency` | Replenishment urgency distribution |
| `GET /fraud-risk` | Fraud score per customer (0–100) |
| `GET /fraud-risk/summary` | Fraud metrics by risk level and segment |
| `GET /channel-comparison` | Hourly ATM vs card vs wallet metrics |
| `GET /channel-comparison/daily` | Daily channel aggregation |
| `GET /channel-comparison/by-time` | Channel performance by time of day |
| `GET /customer-spending` | Spending profile per customer |
| `GET /customer-spending/summary` | Customer segments summary |
| `GET /governorate-summary` | Regional roll-up by Moroccan governorate |

### Observability Endpoints (SLO-tracked)

| Endpoint | What it checks | SLO |
|---|---|---|
| `GET /pipeline-health` | Last dbt run results (reads `run_results.json`) | 100% test pass rate |
| `GET /backup-health` | DuckDB + Postgres backup age | < 26 hours |
| `GET /freshness` | Age of latest landing file per domain | < 15 minutes |
| `GET /kafka-lag` | Consumer lag per Kafka topic | < 500 messages |

---

## CI/CD

### CI — dbt compile + test (`.github/workflows/dbt_ci.yml`)

Triggers on every push or pull request to `main`/`dev` that touches `banking_dbt/`. Spins up a fresh Ubuntu runner, installs `dbt-duckdb`, and runs:

```
dbt deps → dbt compile → dbt test
```

Failing models upload dbt logs as a downloadable GitHub Actions artifact.

### CD — dbt docs to GitHub Pages (`.github/workflows/dbt_docs_cd.yml`)

Triggers on every push to `main` that touches `banking_dbt/`. Generates the full dbt docs site and deploys it automatically to GitHub Pages.

📖 **Live docs:** `https://Mahmoud2saad.github.io/End_To_End_Banking_Pipeline`

---

## Cloud Deployment (Azure + Databricks)

Set `ENV=cloud` in `.env` to activate the cloud path.

1. Provision Azure Data Lake Storage Gen2 with hierarchical namespace enabled
2. Create containers: `landing`, `bronze`, `silver`, `gold`
3. Create a Databricks workspace and configure a cluster with Delta Lake and dbt
4. Set all `AZURE_*` and `DATABRICKS_*` variables in `.env`
5. DAGs `01_bronze_cloud.py` and `02_silver_cloud.py` run as Databricks jobs via `databricks-sdk`
6. dbt targets Databricks via `dbt-databricks` — same models, same tests

---

## Local Setup (Docker + Airflow)

### Prerequisites
- Docker Desktop ≥ 4.x (WSL 2 backend on Windows)
- 8 GB RAM allocated to Docker
- Java 17 (handled automatically inside the container)

### First Run

```bash
# 1. Clone the repo
git clone https://github.com/Mahmoud2saad/End_To_End_Banking_Pipeline.git
cd End_To_End_Banking_Pipeline

# 2. Copy and fill environment variables
cp .env.example .env

# 3. Initialise Airflow (first time only)
docker compose up airflow-init

# 4. Start the stack
docker compose up -d

# 5. Open Airflow UI
open http://localhost:8080
# Username: airflow  Password: airflow
```

### Trigger a Pipeline Run

```bash
# Via CLI:
docker compose exec airflow-webserver airflow dags trigger 00_generate_data
docker compose exec airflow-webserver airflow dags trigger 04_full_pipeline

# Start the Grafana API (separate terminal):
python -m uvicorn grafana_api.main:app --host 0.0.0.0 --port 8000 --reload

# Start Grafana:
docker run -d --name grafana -p 3000:3000 \
  -e GF_INSTALL_PLUGINS=yesoreyeram-infinity-datasource \
  grafana/grafana
# Open http://localhost:3000  (admin / admin)
```

### Stop

```bash
docker compose down       # keep DB
docker compose down -v    # wipe DB and start fresh
```

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ENV` | Yes | `local` or `cloud` |
| `ALERT_EMAIL` | Yes | Airflow failure email recipient |
| `OUTLOOK_PASSWORD` | Yes (local) | SMTP password for Outlook |
| `DUCKDB_PATH` | Optional | Path to DuckDB warehouse file |
| `DBT_RUN_RESULTS` | Optional | Path to dbt `run_results.json` |
| `SIMULATOR_MODE` | Optional | `demo` / `realistic` / `full` (default `demo`) |
| `SIMULATOR_LIMIT_ROWS` | Optional | Row cap per table in demo mode (default 2000) |
| `KAFKA_TOPIC_PREFIX` | Optional | Topic namespace (default `banking.dev`) |
| `KAFKA_RETENTION_HOURS` | Optional | Kafka log retention for replay recovery |
| `AZURE_STORAGE_ACCOUNT_NAME` | Cloud only | ADLS Gen2 account name |
| `AZURE_STORAGE_ACCOUNT_KEY` | Cloud only | ADLS access key |
| `AZURE_TENANT_ID` | Cloud only | Azure AD tenant |
| `AZURE_CLIENT_ID` | Cloud only | Service principal client ID |
| `AZURE_CLIENT_SECRET` | Cloud only | Service principal secret |
| `DATABRICKS_HOST` | Cloud only | Databricks workspace URL |
| `DATABRICKS_TOKEN` | Cloud only | Personal access token |
| `DATABRICKS_CLUSTER_ID` | Cloud only | Interactive cluster ID |

---

## DAG Reference

| DAG | Schedule | Description |
|---|---|---|
| `00_generate_data` | Manual | Runs stream simulator; writes Parquet to landing zone |
| `01_bronze_ingestion` | `*/30 * * * *` | Lands Parquet into Bronze Delta |
| `02_silver_transformation` | `15,45 * * * *` | Cleans Bronze → Silver; 8 tables |
| `03_dbt_gold` | `30 * * * *` | seed → snapshot → staging → marts → test |
| `04_full_pipeline` | Manual | Full E2E run in one DAG |
| `05_kafka_streaming` | Manual | Kafka producer + consumer lifecycle |

---

## SLOs

Defined in `docs/SLOS.md` and monitored via the observability API:

| Metric | Target |
|---|---|
| dbt test pass rate | 100% |
| Backup age (DuckDB + Postgres) | < 26 hours |
| Landing file freshness per domain | < 15 minutes (during active streaming) |
| Kafka consumer lag per topic | < 500 messages |

---

## Troubleshooting

**Docker Airflow fails to start**
```bash
docker compose logs airflow-init
```
Ensure port 8080 is free and `FERNET_KEY` is set or left empty.

**PySpark task fails with `JAVA_HOME not set`**
```bash
docker compose build --no-cache
docker compose up -d
```

**`uvicorn` not found**
```bash
python -m uvicorn grafana_api.main:app --host 0.0.0.0 --port 8000 --reload
```
Use `python -m uvicorn` to avoid PATH resolution issues on Windows.

**`/kafka-lag` returns `NoBrokersAvailable`**
Start Kafka first: `cd kafka && docker compose up -d`

**dbt version conflict with Airflow's `sqlparse`**
Stay on `dbt-core==1.7.4` — Airflow 2.8.1 pins `sqlparse==0.4.4` which is incompatible with dbt-core 1.8+.

---

## Contributors

Built by [Mahmoud Saad](https://github.com/Mahmoud2saad) · [Alfred Farag](https://github.com/af50) · [Mariam Safwat](https://github.com/mariamsafwa) · [Zainab Mohamed](https://github.com/Zainab-Mohammed)
