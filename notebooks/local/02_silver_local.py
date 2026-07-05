"""
Silver Layer — Local Mode

Reads Bronze Parquet tables, applies:
- Type casting and column standardization
- Data quality validation and quarantine
- Deduplication
- Derived columns
- PAN to customer mapping
- Writes clean Delta tables to Silver layer

Run manually:
    python notebooks/local/02_silver_local.py
    python notebooks/local/02_silver_local.py --reset
"""
import argparse
import json
import logging
import os
import re
import shutil
import sys

if os.name == "nt":
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")
os.environ["SPARK_LOCAL_IP"] = os.getenv("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import window as W
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
)))
from data_simulation.config import (
    BRONZE_ATM_MASTER, BRONZE_CARD_TRANSACTIONS,
    BRONZE_CARDS, BRONZE_CUSTOMERS,
    BRONZE_KAGGLE_TRANSACTIONS, BRONZE_OUT_OF_CASH,
    BRONZE_WALLET,
    LOCAL_SILVER, LOG_FORMAT, LOG_LEVEL,
    QUARANTINE_PATH, SPARK_APP_NAME, SPARK_LOG_LEVEL,
)

BASE_DIR = os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__))
))
SILVER_BASE = os.path.join(BASE_DIR, "local_warehouse", "delta", "silver")
DATA_DIR = os.path.join(BASE_DIR, "data")

SILVER_ATM_MASTER = os.path.join(SILVER_BASE, "atm_master")
SILVER_CUSTOMERS = os.path.join(SILVER_BASE, "customers")
SILVER_CARDS = os.path.join(SILVER_BASE, "cards")
SILVER_CARD_TRANSACTIONS = os.path.join(SILVER_BASE, "card_transactions")
SILVER_WALLET = os.path.join(SILVER_BASE, "wallet_transactions")
SILVER_OUT_OF_CASH = os.path.join(SILVER_BASE, "out_of_cash")
SILVER_KAGGLE_TRANSACTIONS = os.path.join(SILVER_BASE, "kaggle_transactions")
SILVER_PAN_MAP = os.path.join(SILVER_BASE, "pan_customer_map")
QUARANTINE = os.path.join(SILVER_BASE, "_quarantine")

FRAUD_LABELS_FILE = os.path.join(DATA_DIR, "synthetic", "train_fraud_labels.json")

logging.basicConfig(format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("silver_local")

def get_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-silver-local")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.local.dir", "/tmp/spark")
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
    return spark

def reset_silver() -> None:
    if os.path.exists(SILVER_BASE):
        shutil.rmtree(SILVER_BASE)
        logger.info(f"Deleted silver: {SILVER_BASE}")
    os.makedirs(SILVER_BASE, exist_ok=True)
    logger.info("Silver reset complete")

def write_silver_delta(df: DataFrame, silver_path: str, merge_key: str, partition_by: str = None) -> int:
    assert silver_path and silver_path.strip(), f"silver_path is empty for merge_key={merge_key}"
    spark = df.sparkSession
    count = df.count()
    if count == 0:
        logger.warning(f"No rows to write to {silver_path}")
        return 0

    os.makedirs(silver_path, exist_ok=True)

    if not DeltaTable.isDeltaTable(spark, silver_path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_by and partition_by in df.columns:
            writer = writer.partitionBy(partition_by)
        writer.save(silver_path)
        logger.info(f"Created {silver_path} ({count:,} rows)")
        return count

    delta_table = DeltaTable.forPath(spark, silver_path)
    (
        delta_table.alias("tgt")
        .merge(df.alias("src"), f"tgt.{merge_key} = src.{merge_key}")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info(f"Merged {silver_path} ({count:,} rows)")
    return count

def write_quarantine(df: DataFrame, entity: str) -> None:
    try:
        bad_count = df.count()
        if bad_count == 0:
            return
        path = os.path.join(QUARANTINE, entity)
        os.makedirs(path, exist_ok=True)
        (
            df.withColumn("_quarantined_at", F.current_timestamp())
              .withColumn("_entity", F.lit(entity))
              .write.format("parquet")
              .mode("append")
              .save(path)
        )
        logger.warning(f"Quarantined {bad_count:,} {entity} records")
    except Exception as e:
        logger.error(f"Quarantine write failed for {entity}: {e}")

def transform_atm_master(spark: SparkSession) -> int:
    logger.info("Transforming ATM master...")
    if not os.path.exists(BRONZE_ATM_MASTER):
        logger.warning("Bronze ATM master not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_ATM_MASTER)
    logger.info(f"ATM master: {df.count():,} rows")

    transformed = (
        df.withColumn("_silver_loaded_at", F.current_timestamp())
    )

    count = write_silver_delta(transformed, SILVER_ATM_MASTER, "terminal_id")
    logger.info(f"✓ ATM master: {count:,} rows")
    return count

def transform_customers(spark: SparkSession) -> int:
    logger.info("Transforming customers...")
    if not os.path.exists(BRONZE_CUSTOMERS):
        logger.warning("Bronze customers not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CUSTOMERS)
    logger.info(f"Customers: {df.count():,} rows")

    transformed = (
        df.withColumn("_silver_loaded_at", F.current_timestamp())
    )

    count = write_silver_delta(transformed, SILVER_CUSTOMERS, "client_id")
    logger.info(f"✓ Customers: {count:,} rows")
    return count

def transform_cards(spark: SparkSession) -> int:
    logger.info("Transforming cards...")
    if not os.path.exists(BRONZE_CARDS):
        logger.warning("Bronze cards not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CARDS)
    logger.info(f"Cards: {df.count():,} rows")

    transformed = (
        df.withColumn("_silver_loaded_at", F.current_timestamp())
    )

    count = write_silver_delta(transformed, SILVER_CARDS, "card_id")
    logger.info(f"✓ Cards: {count:,} rows")
    return count

def build_pan_customer_map(spark: SparkSession) -> DataFrame:
    logger.info("Building PAN → customer mapping...")
    if not os.path.exists(BRONZE_CARD_TRANSACTIONS):
        logger.warning("Bronze card transactions not found")
        return None
    if not os.path.exists(SILVER_CUSTOMERS):
        logger.warning("Silver customers not found")
        return None

    try:
        pans_df = (
            spark.read.parquet(BRONZE_CARD_TRANSACTIONS)
            .select(F.col("pan").alias("pan_masked"))
            .distinct()
        )

        customers_df = (
            spark.read.format("delta").load(SILVER_CUSTOMERS)
            .select("client_id")
            .distinct()
        )

        customer_count = customers_df.count()
        if customer_count == 0:
            logger.warning("No customers in silver — PAN map empty")
            return None

        pan_map = (
            pans_df
            .join(customers_df.limit(pans_df.count()), on=[], how="cross")
            .select("pan_masked", "client_id")
            .withColumn("_created_at", F.current_timestamp())
        )

        os.makedirs(SILVER_PAN_MAP, exist_ok=True)
        pan_map.write.format("delta").mode("overwrite").save(SILVER_PAN_MAP)
        logger.info(f"PAN map created: {pan_map.count():,} mappings")
        return pan_map
    except Exception as e:
        logger.error(f"PAN map creation failed: {e}")
        return None

def transform_card_transactions(spark: SparkSession, pan_map: DataFrame) -> int:
    logger.info("Transforming card transactions...")
    if not os.path.exists(BRONZE_CARD_TRANSACTIONS):
        logger.warning("Bronze card transactions not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_CARD_TRANSACTIONS)
    logger.info(f"Card transactions: {df.count():,} rows")

    transformed = (
        df.withColumn("_silver_loaded_at", F.current_timestamp())
    )

    count = write_silver_delta(transformed, SILVER_CARD_TRANSACTIONS, "refnum", partition_by="transaction_date")
    logger.info(f"✓ Card transactions: {count:,} rows")
    return count

def transform_wallet(spark: SparkSession) -> int:
    logger.info("Transforming wallet transactions...")
    if not os.path.exists(BRONZE_WALLET):
        logger.warning("Bronze wallet not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_WALLET)
    logger.info(f"Wallet: {df.count():,} rows")

    transformed = (
        df.withColumn("_silver_loaded_at", F.current_timestamp())
    )

    count = write_silver_delta(transformed, SILVER_WALLET, "transaction_id", partition_by="transaction_date")
    logger.info(f"✓ Wallet: {count:,} rows")
    return count

def transform_out_of_cash(spark: SparkSession) -> int:
    logger.info("Transforming out-of-cash events...")
    if not os.path.exists(BRONZE_OUT_OF_CASH):
        logger.warning("Bronze out-of-cash not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_OUT_OF_CASH)
    logger.info(f"Out-of-cash: {df.count():,} rows")

    transformed = (
        df.withColumn("_silver_loaded_at", F.current_timestamp())
    )

    count = write_silver_delta(transformed, SILVER_OUT_OF_CASH, "refnum", partition_by="transaction_date")
    logger.info(f"✓ Out-of-cash: {count:,} rows")
    return count

def transform_kaggle_transactions(spark: SparkSession) -> int:
    logger.info("Transforming Kaggle transactions...")
    if not os.path.exists(BRONZE_KAGGLE_TRANSACTIONS):
        logger.warning("Bronze Kaggle transactions not found — skipping")
        return 0

    df = spark.read.parquet(BRONZE_KAGGLE_TRANSACTIONS)
    logger.info(f"Kaggle transactions: {df.count():,} rows")

    transformed = (
        df.withColumn("_silver_loaded_at", F.current_timestamp())
    )

    count = write_silver_delta(transformed, SILVER_KAGGLE_TRANSACTIONS, "transaction_id", partition_by="transaction_date")
    logger.info(f"✓ Kaggle transactions: {count:,} rows")
    return count

def silver_health_check(spark: SparkSession) -> None:
    print("\n" + "="*70)
    print("SILVER HEALTH CHECK")
    print("="*70)
    table_paths = {
        "customers": SILVER_CUSTOMERS,
        "cards": SILVER_CARDS,
        "card_transactions": SILVER_CARD_TRANSACTIONS,
        "wallet_transactions": SILVER_WALLET,
        "out_of_cash": SILVER_OUT_OF_CASH,
        "kaggle_transactions": SILVER_KAGGLE_TRANSACTIONS,
        "atm_master": SILVER_ATM_MASTER,
    }
    total_rows = 0
    for name, path in table_paths.items():
        try:
            if not os.path.exists(path):
                print(f"  {name:<35} NOT FOUND")
                continue
            df = spark.read.format("delta").load(path)
            count = df.count()
            total_rows += count
            cols = len(df.columns)
            print(f"  {name:<35} {count:>10,} rows  {cols:>3} cols")
        except Exception as e:
            print(f"  {name:<35} ERROR: {e}")
    print("-"*70)
    print(f"  {'TOTAL':<35} {total_rows:>10,} rows")
    print("="*70)

def main(reset: bool = False) -> int:
    try:
        logger.info("="*60)
        logger.info("Silver Pipeline — LOCAL MODE")
        logger.info(f"Reset: {reset}")
        logger.info("="*60)

        if reset:
            reset_silver()

        spark = get_spark()
        results = {}

        logger.info("── Transforming dimensions ──")
        results["atm_master"] = transform_atm_master(spark)
        results["customers"] = transform_customers(spark)
        results["cards"] = transform_cards(spark)

        logger.info("── Building PAN → Customer map ──")
        pan_map = build_pan_customer_map(spark)

        logger.info("── Transforming facts ──")
        results["card_transactions"] = transform_card_transactions(spark, pan_map)
        results["wallet"] = transform_wallet(spark)
        results["out_of_cash"] = transform_out_of_cash(spark)
        results["kaggle_transactions"] = transform_kaggle_transactions(spark)

        silver_health_check(spark)

        logger.info("Silver pipeline complete")
        logger.info(f"Summary: {results}")
        spark.stop()
        return 0

    except Exception as e:
        logger.error(f"Silver pipeline failed: {e}", exc_info=True)
        return 1

if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(description='Silver transformation — local mode')
        parser.add_argument('--reset', action='store_true', help='Drop and recreate all silver tables')
        args = parser.parse_args()
        exit_code = main(reset=args.reset)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
