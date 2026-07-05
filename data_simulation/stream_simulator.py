# data_simulation/stream_simulator.py
"""
Stream Simulator for Banking Pipeline.

Replays CSV files as a live stream by writing small batches
to the landing zone at a configurable interval.

LOCAL mode  → writes Parquet batches to local_warehouse/delta/landing/
CLOUD mode  → writes Parquet batches to ADLS Gen2 landing zone

Usage:
    # Local — all tables, no delay (for testing)
    python data_simulation/stream_simulator.py --delay 0 --max-batches 5

    # Local — specific table
    python data_simulation/stream_simulator.py --table card_transactions --max-batches 3

    # Local — full run
    python data_simulation/stream_simulator.py

    # Cloud
    ENV=cloud python data_simulation/stream_simulator.py
"""
import argparse
import logging
import os
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
import pandas as pd

if os.name == "nt":
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")
os.environ["SPARK_LOCAL_IP"] = os.getenv("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"]  = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from data_simulation.config import (
    ATM_MASTER_FILE, CARDS_FILE, CARDS_TXN_FILE,
    KAGGLE_TRANSACTIONS, LOG_FORMAT, LOG_LEVEL,
    OUT_OF_CASH_FILE, SIMULATOR_BATCH_SIZE,
    SIMULATOR_DELAY_SECS, SIMULATOR_MAX_BATCHES,
    SPARK_APP_NAME, SPARK_LOG_LEVEL, SPARK_MASTER,
    USERS_FILE, WALLET_FILE,
    LANDING_CARD_TRANSACTIONS, LANDING_WALLET,
    LANDING_OUT_OF_CASH, LANDING_CUSTOMERS,
    LANDING_CARDS, LANDING_ATM_MASTER,
    LANDING_KAGGLE_TRANSACTIONS, IS_LOCAL,
)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("stream_simulator")


# ── Spark ─────────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-simulator")
        .master(SPARK_MASTER)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.local.dir", "/tmp/spark")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.network.timeout", "600s")
    )
    if IS_LOCAL:
        builder = builder.config(
            "spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0"
        )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark


# ── Column cleaner ────────────────────────────────────────────────────────────
def clean_col_name(name: str) -> str:
    """Replaces special characters in column names with underscores."""
    return re.sub(r'[ ,;{}()\n\t=]', '_', name).lower().strip('_')


# ── Writer ────────────────────────────────────────────────────────────────────
def write_batch(
    spark: SparkSession,
    pdf: pd.DataFrame,
    output_path: str,
    batch_num: int,
    table_name: str,
) -> None:
    """
    Writes one pandas batch as Parquet to the landing zone.
    - Cleans column names
    - Drops unnamed index columns
    - Adds pipeline metadata columns
    - Writes to a uniquely named subfolder
    """
    now = datetime.now(timezone.utc)
    batch_id = str(uuid.uuid4())[:8]
    ts = now.strftime("%Y%m%d_%H%M%S")

    # drop pandas index columns
    pdf = pdf.loc[:, ~pdf.columns.str.startswith("Unnamed")]

    # drop empty columns (like the '.' column in out_of_cash)
    pdf = pdf.loc[:, pdf.columns.str.strip() != '.']
    pdf = pdf.loc[:, pdf.columns.str.strip() != '']

    # clean column names
    pdf.columns = [clean_col_name(c) for c in pdf.columns]

    # add metadata
    pdf["_batch_id"]     = batch_id
    pdf["_batch_num"]    = batch_num
    pdf["_ingested_at"]  = now.isoformat()
    pdf["_source_table"] = table_name

    # infer schema from pandas — Bronze takes raw data as-is
    # Silver does all the cleaning and type casting
    sdf = spark.createDataFrame(pdf)

    file_path = os.path.join(
        output_path,
        f"{table_name}_{ts}_{str(batch_num).zfill(6)}_{batch_id}"
    )

    if IS_LOCAL:
        os.makedirs(output_path, exist_ok=True)

    (
        sdf.write
        .format("parquet")
        .mode("overwrite")
        .save(file_path)
    )


# ── Core simulator ────────────────────────────────────────────────────────────
def simulate_table(
    spark: SparkSession,
    source_file: str,
    output_path: str,
    table_name: str,
    batch_size: int = SIMULATOR_BATCH_SIZE,
    delay_seconds: float = SIMULATOR_DELAY_SECS,
    max_batches: Optional[int] = None,
) -> dict:
    """
    Reads a CSV file and writes it in batches to the landing zone.
    Each batch is a separate Parquet file — Bronze Auto Loader / watcher
    picks up each file the moment it appears.
    """
    if not os.path.exists(source_file):
        logger.error(f"Source file not found: {source_file}")
        return {"batches": 0, "rows": 0, "elapsed_seconds": 0}

    logger.info(f"Loading {table_name} from {source_file}")
    
    # Sample large files to avoid memory issues
    if table_name == "kaggle_transactions":
        logger.info(f"  {table_name} is large — sampling 5000 rows")
        pdf = pd.read_csv(source_file, low_memory=False, nrows=5000)
    else:
        pdf = pd.read_csv(source_file, low_memory=False)

    # drop unnamed index columns immediately
    pdf = pdf.loc[:, ~pdf.columns.str.startswith("Unnamed")]
    pdf = pdf.loc[:, pdf.columns.str.strip() != '.']

    total_rows    = len(pdf)
    total_batches = (total_rows + batch_size - 1) // batch_size
    if max_batches:
        total_batches = min(total_batches, max_batches)

    logger.info(
        f"{table_name}: {total_rows:,} rows → "
        f"{total_batches:,} batches of {batch_size}"
    )

    stats = {
        "batches": 0, "rows": 0,
        "start": datetime.now(timezone.utc)
    }

    try:
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            batch_pdf = pdf.iloc[start_idx:start_idx + batch_size].copy()

            write_batch(spark, batch_pdf, output_path, batch_num, table_name)

            stats["batches"] += 1
            stats["rows"]    += len(batch_pdf)

            if batch_num % 20 == 0 or batch_num == total_batches - 1:
                elapsed = (
                    datetime.now(timezone.utc) - stats["start"]
                ).seconds
                pct = (batch_num + 1) / total_batches * 100
                logger.info(
                    f"{table_name} | {pct:5.1f}% | "
                    f"batch {batch_num+1}/{total_batches} | "
                    f"rows {stats['rows']:,} | {elapsed}s"
                )

            time.sleep(delay_seconds)

    except KeyboardInterrupt:
        logger.info(
            f"{table_name}: stopped by user "
            f"after {stats['batches']} batches"
        )

    elapsed = (datetime.now(timezone.utc) - stats["start"]).seconds
    stats["elapsed_seconds"] = elapsed
    logger.info(
        f"✓ {table_name}: {stats['batches']} batches, "
        f"{stats['rows']:,} rows, {elapsed}s"
    )
    return stats


# ── Static loader ─────────────────────────────────────────────────────────────
def load_static_tables(spark: SparkSession) -> None:
    """
    Writes dimension tables to landing zone once at pipeline start.
    ATM master, customers, cards don't need streaming —
    they change infrequently and are loaded in full each run.
    """
    static_tables = [
        (ATM_MASTER_FILE, LANDING_ATM_MASTER, "atm_master"),
        (USERS_FILE,      LANDING_CUSTOMERS,  "customers"),
        (CARDS_FILE,      LANDING_CARDS,      "cards"),
    ]

    for source, output, name in static_tables:
        logger.info(f"Loading static table: {name}")
        if not os.path.exists(source):
            logger.warning(f"File not found — skipping: {source}")
            continue

        pdf = pd.read_csv(source, low_memory=False)
        logger.info(f"  {name}: {len(pdf):,} rows")

        write_batch(spark, pdf, output, 0, name)
        logger.info(f"  ✓ {name} written to landing")


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Banking Pipeline Stream Simulator"
    )
    parser.add_argument(
        "--table",
        choices=[
            "card_transactions", "wallet",
            "out_of_cash", "kaggle_transactions", "all"
        ],
        default="all",
        help="Which streaming table to run (default: all)"
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=SIMULATOR_MAX_BATCHES,
        help="Max batches per table — None means full file"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=SIMULATOR_BATCH_SIZE,
        help=f"Rows per batch (default: {SIMULATOR_BATCH_SIZE})"
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=SIMULATOR_DELAY_SECS,
        help=f"Seconds between batches (default: {SIMULATOR_DELAY_SECS})"
    )
    parser.add_argument(
        "--skip-static",
        action="store_true",
        help="Skip loading static dimension tables"
    )
    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("Banking Pipeline — Stream Simulator")
    logger.info(f"Mode:        {'LOCAL' if IS_LOCAL else 'CLOUD'}")
    logger.info(f"Table:       {args.table}")
    logger.info(f"Batch size:  {args.batch_size}")
    logger.info(f"Delay:       {args.delay}s")
    logger.info(f"Max batches: {args.max_batches or 'unlimited'}")
    logger.info("=" * 60)

    spark = get_spark()

    # Step 1 — static dimension tables
    if not args.skip_static:
        logger.info("Step 1 — Loading static dimension tables")
        load_static_tables(spark)
    else:
        logger.info("Step 1 — Skipping static tables (--skip-static)")

    # Step 2 — streaming fact tables
    streaming_tables = {
        "card_transactions": (
            CARDS_TXN_FILE,
            LANDING_CARD_TRANSACTIONS,
            "card_transactions",
        ),
        "wallet": (
            WALLET_FILE,
            LANDING_WALLET,
            "wallet_transactions",
        ),
        "out_of_cash": (
            OUT_OF_CASH_FILE,
            LANDING_OUT_OF_CASH,
            "out_of_cash",
        ),
        "kaggle_transactions": (
            KAGGLE_TRANSACTIONS,
            LANDING_KAGGLE_TRANSACTIONS,
            "kaggle_transactions",
        ),
    }

    tables_to_run = (
        list(streaming_tables.keys())
        if args.table == "all"
        else [args.table]
    )

    logger.info(f"Step 2 — Streaming {len(tables_to_run)} table(s)")

    all_stats = {}
    for key in tables_to_run:
        src, output, name = streaming_tables[key]
        all_stats[name] = simulate_table(
            spark=spark,
            source_file=src,
            output_path=output,
            table_name=name,
            batch_size=args.batch_size,
            delay_seconds=args.delay,
            max_batches=args.max_batches,
        )

    # summary
    print()
    print("=" * 65)
    print("SIMULATION COMPLETE")
    print("=" * 65)
    total_rows    = sum(s["rows"] for s in all_stats.values())
    total_batches = sum(s["batches"] for s in all_stats.values())
    for name, stats in all_stats.items():
        print(
            f"  {name:<30} {stats['rows']:>10,} rows  "
            f"{stats['batches']:>5} batches  "
            f"{stats['elapsed_seconds']}s"
        )
    print("-" * 65)
    print(
        f"  {'TOTAL':<30} {total_rows:>10,} rows  "
        f"{total_batches:>5} batches"
    )
    print("=" * 65)

    spark.stop()


if __name__ == "__main__":
    main()

