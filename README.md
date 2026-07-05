<div align="center">

# 🏦 End-to-End Banking Pipeline

### A Production-Grade Hybrid Data Engineering Platform

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-Streaming-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://getdbt.com)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.10-FFF000?style=for-the-badge&logo=duckdb&logoColor=black)](https://duckdb.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![License: MIT](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

*A production-ready, end-to-end data engineering platform for banking analytics — runs fully local or fully cloud with the flip of a single environment variable.*

[Architecture](#-architecture) · [Quick Start](#-quick-start) · [Tech Stack](#-tech-stack) · [Data Model](#-data-model) · [DAG Reference](#-dag-reference) · [Troubleshooting](#-troubleshooting)

</div>

---

## 📐 Architecture

The platform follows a **Medallion Architecture** (Landing → Bronze → Silver → Gold) with real-time streaming via Kafka and CDC-powered replication to Snowflake via Debezium.

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                         BANKING PIPELINE — HYBRID ARCHITECTURE                   │
│                                                                                  │
│  ┌──────────────┐    ┌───────────────────────────────────────────────────────┐   │
│  │  Simulation  │───▶│              APACHE KAFKA (Real-time)                 │   │
│  │  / CDC Source│    │  Transactions · ATM Events · Wallet Ops · Fraud Sig.  │   │
│  └──────────────┘    └────────────────────┬──────────────────────────────────┘   │
│                                           │ Kafka → Snowflake CDC               │
│           ┌───────────────────────────────▼──────────────────────────────────┐   │
│           │                 LANDING ZONE  (Parquet / JSON)                   │   │
│           └───────────────────────────────┬──────────────────────────────────┘   │
│                                           │                                      │
│           ┌───────────────────────────────▼──────────────────────────────────┐   │
│           │               BRONZE LAYER  (Raw Delta / Parquet)                │   │
│           │         Schema-on-read · Append-only · Full audit trail          │   │
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
│           ┌──────────────┐   ┌────────────▼────────────┐   ┌──────────────────┐  │
│           │   Grafana    │   │       Snowflake          │   │   Power BI /     │  │
│           │  Monitoring  │   │   (Cloud Data Warehouse) │   │   Dashboards     │  │
│           └──────────────┘   └─────────────────────────┘   └──────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

> 📸 See [`architecture.png`](architecture.png), [`schema.png`](schema.png), and [`modeling.png`](modeling.png) for full visual diagrams.

---

## ✨ Key Features

| Feature | Details |
|---|---|
| 🔀 **Hybrid Execution** | Toggle `ENV=local` (DuckDB + local Spark) or `ENV=cloud` (Azure ADLS + Databricks) — same DAGs, same dbt models |
| 🥉🥈🥇 **Medallion Architecture** | Landing → Bronze → Silver → Gold with strict layer contracts |
| ⚡ **Real-time Streaming** | Apache Kafka ingests card transactions, ATM events, and wallet operations |
| 🔄 **CDC Pipeline** | Debezium captures source DB changes → Kafka → Snowflake via Kafka Connect |
| 🧪 **dbt Gold Layer** | 6 dimensions, 4 fact tables, 6 analytics marts — all tested with dbt's built-in quality suite |
| 🎛️ **Orchestration** | Apache Airflow 2.8.1 with ExternalTaskSensor chaining, email alerting, and custom operators |
| 🐳 **Dockerised Airflow** | One-command local stack: Postgres metadata DB + Airflow webserver + scheduler |
| 📊 **Grafana Monitoring** | Real-time pipeline health, ingestion rates, and anomaly detection |
| 🏭 **Fabric / Lakehouse** | Alternative Microsoft Fabric path for unified SaaS analytics |

---

## 🗂️ Project Structure

```
Banking_Pipeline/
├── airflow/                              # Dockerised Airflow
│   ├── dags/
│   │   ├── 00_generate_data_dag.py       # Manual: stream simulator → landing zone
│   │   ├── 01_bronze_dag.py              # Scheduled: landing → Bronze (every 30 min)
│   │   ├── 02_silver_dag.py              # Scheduled: Bronze → Silver (offset 15 min)
│   │   ├── 03_dbt_dag.py                 # Scheduled: Silver → Gold via dbt (hourly)
│   │   └── 04_full_pipeline_dag.py       # Single DAG: full E2E run
│   ├── plugins/operators/
│   │   ├── spark_operator.py             # SparkSubmitLocalOperator
│   │   └── dbt_operator.py               # DbtOperator wrapping dbt CLI
│   └── logs/
│
├── notebooks/
│   ├── local/
│   │   ├── 01_bronze_local.py            # PySpark: landing → Bronze Delta
│   │   └── 02_silver_local.py            # PySpark: Bronze → Silver Delta
│   └── cloud/
│       ├── 01_bronze_cloud.py            # Databricks: ADLS landing → Bronze
│       └── 02_silver_cloud.py            # Databricks: Bronze → Silver
│
├── banking_dbt/                          # dbt project
│   ├── models/
│   │   ├── staging/                      # Cleaned Silver views (tag:staging)
│   │   ├── marts/
│   │   │   ├── dimensions/               # dim_customer, dim_card, dim_atm, dim_date…
│   │   │   └── facts/                    # fact_card_transactions, fact_atm…
│   │   └── gold/                         # Analytics marts: fraud_risk, atm_performance…
│   ├── snapshots/                        # SCD Type 2 snapshots
│   ├── seeds/                            # Reference data
│   └── profiles.yml
│
├── data_simulation/
│   └── stream_simulator.py               # Faker-based Kafka producer
│
├── kafka/
│   ├── producer/                         # Kafka producer configs
│   ├── consumer/                         # Kafka consumer → Snowflake sink
│   └── debezium/                         # CDC connector configs
│
├── grafana/
│   ├── dashboards/                       # Pre-built JSON dashboard exports
│   └── provisioning/
│
├── local_warehouse/                      # DuckDB file + Delta tables (gitignored)
├── docker-compose.yml                    # Airflow stack
├── Dockerfile                            # Airflow 2.8.1 + Java 17 + PySpark + dbt
├── requirements.airflow.txt              # Container dependencies
├── .env.example                          # Template for secrets
└── Makefile                              # Convenience targets
```

---

## 🛠️ Tech Stack

### Core Runtime

| Layer | Local | Cloud |
|---|---|---|
| **Storage** | Delta Lake (local FS) | Azure Data Lake Storage Gen2 |
| **Processing** | PySpark 3.5 `local[*]` | Azure Databricks |
| **Warehouse** | DuckDB 0.10 | Snowflake |
| **Transformation** | dbt-duckdb 1.7 | dbt-databricks 1.7 |
| **Streaming** | Apache Kafka | Azure Event Hubs (Kafka-compatible) |
| **CDC** | Debezium + Kafka Connect | Debezium + Azure Event Hubs |
| **Orchestration** | Apache Airflow 2.8.1 (Docker) | Apache Airflow 2.8.1 (Docker) |
| **Monitoring** | Grafana | Grafana |
| **Alt. Lakehouse** | — | Microsoft Fabric |

### Key Libraries

```
pyspark==3.5.0            delta-spark==3.0.0
dbt-core==1.7.4           dbt-duckdb==1.7.4         dbt-databricks==1.7.2
azure-storage-blob        azure-identity            azure-storage-file-datalake
databricks-sdk==0.20.0    duckdb==0.10.3
faker==22.2.0             pandas==2.1.4             pyarrow==14.0.2
```

---

## 📊 Data Model

The pipeline models banking operations across **5 source domains**:

| Domain | Tables | Description |
|---|---|---|
| **ATM Operations** | `atm_master`, `out_of_cash` | ATM locations, cash status, replenishment events |
| **Customers** | `customers`, `pan_customer_map` | Customer demographics, card-to-customer mapping |
| **Cards** | `cards`, `card_transactions` | Card metadata, all card-present and card-not-present transactions |
| **Wallets** | `wallet_transactions` | Mobile wallet transfers and top-ups |
| **Kaggle Reference** | `kaggle_transactions` | External fraud-labeled transaction dataset for model training |

### Gold Layer — Dimensional Model

**Dimensions**

| Model | Description |
|---|---|
| `dim_customer` | Customer profile with SCD Type 2 history |
| `dim_card` | Card attributes and status |
| `dim_atm` | ATM master data with geographic enrichment |
| `dim_date` | Calendar spine (daily grain) |
| `dim_geography` | Governorate / region hierarchy |
| `dim_merchant` | Merchant category codes and segments |

**Facts**

| Model | Description |
|---|---|
| `fact_card_transactions` | All card transactions with foreign keys to all dimensions |
| `fact_atm_transactions` | ATM withdrawal and deposit events |
| `fact_wallet_transactions` | Wallet-to-wallet and wallet-to-bank transfers |
| `fact_out_of_cash_events` | ATM cash depletion events with time-to-replenishment |

**Analytics Marts (Gold)**

| Mart | Description |
|---|---|
| `atm_performance` | Utilisation rates, cash cycle times, failure rates by ATM |
| `fraud_risk_scoring` | Rule-based and ML-compatible fraud signals per transaction |
| `customer_spending_behavior` | RFM scoring, channel preferences, segment tags |
| `replenishment_analysis` | Cash replenishment SLA adherence by region |
| `channel_comparison` | ATM vs card vs wallet volume and value by month |
| `governorate_summary` | Regional transaction heatmap for operations dashboards |

---

## 🥉🥈🥇 Medallion Layers

### Landing Zone
Raw files from the stream simulator or CDC consumer. Parquet by default, partitioned by `ingestion_date`. No schema enforcement — append only.

### Bronze Layer
PySpark reads from landing and writes to Delta with schema enforcement. Full audit columns are added: `_source_file`, `_ingested_at`, `_pipeline_run_id`. Deduplication is **not** performed here — Bronze is immutable.

### Silver Layer
PySpark reads Bronze Delta and applies:
- Null handling and type casting
- Deduplication on natural keys
- Standardised column naming
- SCD Type 2 tracking markers

Silver tables: `atm_master`, `customers`, `cards`, `card_transactions`, `wallet_transactions`, `out_of_cash`, `kaggle_transactions`, `pan_customer_map`.

### Gold Layer (dbt)
dbt runs `seed → snapshot → tag:staging → tag:marts → test`. All models are materialised as tables in DuckDB locally and as Delta tables in Databricks. dbt tests cover: not-null, unique, referential integrity, accepted values, and custom data quality assertions.

---

## 🎛️ Airflow Orchestration

### DAG Chain

```
00_generate_data        (manual trigger)
         │
         ▼
01_bronze_ingestion     (*/30 * * * *)
         │  ExternalTaskSensor
         ▼
02_silver_transformation  (15,45 * * * *)
         │  ExternalTaskSensor
         ▼
03_dbt_gold             (30 * * * *)
```

> Run **`04_full_pipeline`** to execute the full chain in a single DAG for development and testing.

### Alerting
Every DAG sends email alerts on failure via Outlook SMTP (configurable). Success notifications include table counts and run metadata.

### Custom Operators

| Operator | Description |
|---|---|
| `SparkSubmitLocalOperator` | Runs any PySpark script as a subprocess with full `JAVA_HOME` and `PYTHONPATH` injection |
| `DbtOperator` | Wraps `dbt run / test / seed / snapshot` with profiles and project dir overrides |

---

## ⚡ Kafka Streaming & CDC

### Real-time Streaming

`stream_simulator.py` produces synthetic banking events to the following Kafka topics:

| Topic | Schema | Rate |
|---|---|---|
| `banking.card_transactions` | Transaction amount, merchant, card PAN, timestamp | Configurable |
| `banking.atm_events` | ATM ID, event type, cash level | Configurable |
| `banking.wallet_ops` | Wallet ID, transfer type, counterparty | Configurable |

Consumers write to the landing zone (local) or Azure Event Hubs (cloud).

### CDC Pipeline (Kafka → Snowflake)

```
Source DB → Debezium → Kafka → Kafka Connect Snowflake Sink → Snowflake Staging → MERGE → Target Tables
```

Debezium monitors the source OLTP database and streams row-level changes (`INSERT / UPDATE / DELETE`) to Kafka. A Kafka Connect Snowflake Sink Connector lands these changes in a Snowflake staging schema. A merge procedure applies the CDC events to the target tables, achieving near-real-time replication with full change history.

---

## ☁️ Cloud Deployment (Azure + Databricks)

Set `ENV=cloud` in `.env` to activate the cloud path.

1. Provision **Azure Data Lake Storage Gen2** with hierarchical namespace enabled
2. Create containers: `landing`, `bronze`, `silver`, `gold`
3. Create a **Databricks workspace** and configure a cluster with Delta Lake and dbt installed
4. Set all `AZURE_*` and `DATABRICKS_*` variables in `.env`
5. DAGs `01_bronze_cloud.py` and `02_silver_cloud.py` run as Databricks jobs via the `databricks-sdk`
6. dbt targets Databricks via `dbt-databricks` — same models, same tests

---

## ❄️ Snowflake Integration

Snowflake serves as the cloud analytical warehouse. The CDC pipeline handles real-time ingestion. dbt models can also be targeted to Snowflake by switching `DBT_TARGET=snowflake` and configuring `profiles.yml` with your Snowflake account credentials.

---

## 📈 Grafana Monitoring

Pre-built dashboards cover:

- **Pipeline health** — DAG run success rates, task duration P95, failure counts
- **Ingestion rates** — rows/min landing in Bronze, Silver lag, Gold build times
- **Data quality** — dbt test pass/fail over time, null rates, duplicate rates
- **Business KPIs** — transaction volumes, ATM cash levels, fraud signal counts

Import the JSON files from `grafana/dashboards/` into your Grafana instance. The datasource is PostgreSQL (Airflow metadata DB) for pipeline metrics and DuckDB (via Grafana DuckDB plugin) for business KPIs.

---

## 🏭 Fabric / Lakehouse Alternative

A **Microsoft Fabric** notebook path is included as an alternative to the Databricks route. Fabric Lakehouses consume the same Delta-format Bronze and Silver outputs. The dbt project targets the Fabric SQL endpoint via `dbt-spark`. This makes the solution a true multi-cloud, multi-engine hybrid — Fabric for Microsoft-native organisations, Databricks for mixed-cloud shops.

---

## 🚀 Quick Start

### Prerequisites

- Docker Desktop ≥ 4.x (WSL 2 backend on Windows)
- 8 GB RAM allocated to Docker
- Java 17 *(handled automatically inside the container)*

### First Run

```bash
# 1. Clone the repo
git clone https://github.com/Mahmoud2Saad/End_To_End_Banking_Pipeline.git
cd End_To_End_Banking_Pipeline

# 2. Copy and fill in environment variables
cp .env.example .env
# Edit .env — set ALERT_EMAIL and OUTLOOK_PASSWORD at minimum

# 3. Initialise Airflow (first time only)
docker compose up airflow-init

# 4. Start the stack
docker compose up -d

# 5. Open the Airflow UI
open http://localhost:8080
# Username: airflow  |  Password: airflow
```

### Trigger a Pipeline Run

```bash
# Via Airflow UI:
# Enable DAG 00_generate_data → trigger manually
# Then enable and watch DAGs 01, 02, 03 run automatically

# Or via CLI:
docker compose exec airflow-webserver airflow dags trigger 00_generate_data
docker compose exec airflow-webserver airflow dags trigger 04_full_pipeline
```

### Stop the Stack

```bash
docker compose down        # Keep DB
docker compose down -v     # Wipe DB and start fresh
```

---

## ⚙️ Environment Variables

| Variable | Required | Description |
|---|---|---|
| `ENV` | ✅ Yes | `local` or `cloud` |
| `ALERT_EMAIL` | ✅ Yes | Airflow failure email recipient |
| `OUTLOOK_PASSWORD` | ✅ Local | SMTP password for Outlook |
| `AZURE_STORAGE_ACCOUNT_NAME` | ☁️ Cloud | ADLS Gen2 account name |
| `AZURE_STORAGE_ACCOUNT_KEY` | ☁️ Cloud | ADLS access key |
| `AZURE_TENANT_ID` | ☁️ Cloud | Azure AD tenant |
| `AZURE_CLIENT_ID` | ☁️ Cloud | Service principal client ID |
| `AZURE_CLIENT_SECRET` | ☁️ Cloud | Service principal secret |
| `DATABRICKS_HOST` | ☁️ Cloud | Databricks workspace URL |
| `DATABRICKS_TOKEN` | ☁️ Cloud | Personal access token |
| `DATABRICKS_HTTP_PATH` | ☁️ Cloud | SQL warehouse HTTP path |
| `DATABRICKS_CLUSTER_ID` | ☁️ Cloud | Interactive cluster ID |
| `SIMULATOR_BATCH_SIZE` | ⚙️ Optional | Rows per simulation batch *(default: 50)* |
| `SIMULATOR_DELAY_SECS` | ⚙️ Optional | Delay between batches *(default: 5.0s)* |
| `SPARK_MASTER` | ⚙️ Optional | Spark master URL *(default: `local[*]`)* |
| `DBT_TARGET` | ⚙️ Optional | dbt target profile *(default: `dev`)* |

---

## 📋 DAG Reference

| DAG | Schedule | Trigger | Description |
|---|---|---|---|
| `00_generate_data` | None | Manual | Runs stream simulator; writes Parquet to landing zone |
| `01_bronze_ingestion` | `*/30 * * * *` | Automatic | Lands Parquet into Bronze Delta; email on success/failure |
| `02_silver_transformation` | `15,45 * * * *` | Waits for Bronze | Cleans Bronze → Silver; 8 tables |
| `03_dbt_gold` | `30 * * * *` | Waits for Silver | `seed → snapshot → staging → marts → test` |
| `04_full_pipeline` | None | Manual | Full E2E run in one DAG for dev/testing |

---

## 🔧 Troubleshooting

<details>
<summary><strong>Docker Airflow fails to start</strong></summary>

**Symptom:** `airflow-init` exits with a non-zero code.

**Fix:** Ensure `FERNET_KEY` is set or left empty (auto-generated). Check that port `8080` is free.

```bash
docker compose logs airflow-init
```
</details>

<details>
<summary><strong>PySpark task fails with JAVA_HOME not set</strong></summary>

**Fix:** The Dockerfile installs `openjdk-17-jdk-headless` and sets `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`. Confirm the container rebuilt after any Dockerfile changes:

```bash
docker compose build --no-cache
docker compose up -d
```
</details>

<details>
<summary><strong>DuckDB version conflict in requirements</strong></summary>

**Note:** `requirements.airflow.txt` may pin two conflicting versions. Remove the duplicate and keep:

```
duckdb==0.10.3
```
</details>

<details>
<summary><strong>ExternalTaskSensor times out</strong></summary>

**Symptom:** `02_silver_transformation` waits indefinitely for `01_bronze_ingestion`.

**Fix:** Ensure both DAGs use the same `schedule_interval` offset and that Bronze completed within the sensor's timeout. Increase `timeout=600` in the sensor if your Bronze run takes longer.
</details>

<details>
<summary><strong>dbt version conflict with Airflow's sqlparse</strong></summary>

**Reason:** Airflow 2.8.1 pins `sqlparse==0.4.4`. dbt-core 1.8+ requires a newer version.

**Fix:** Stay on `dbt-core==1.7.4` (already pinned in `requirements.airflow.txt`).
</details>

---

## 🗺️ Roadmap

- [ ] Add Great Expectations data quality checks at the Silver layer
- [ ] Implement dbt metrics layer for standardised KPI definitions
- [ ] Add Terraform IaC for Azure resource provisioning
- [ ] Integrate MLflow for fraud model experiment tracking
- [ ] Add unit tests for custom Airflow operators
- [ ] Publish Grafana dashboards to Grafana Cloud
- [ ] Add Kubernetes Helm chart for cloud-native Airflow deployment

---

## 📄 License

This project is licensed under the **MIT License** — see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Built with ❤️ by**

[Mahmoud Saad](https://github.com/Mahmoud2Saad) · [Alfred Farag](https://github.com/af50) · [Mariam Safwat](https://github.com/mariamsafwa) · [Zainab Mohamed](https://github.com/Zainab-Mohammed)

</div>
