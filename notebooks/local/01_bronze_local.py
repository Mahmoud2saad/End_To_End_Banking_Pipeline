#!/usr/bin/env python3
"""Bronze Layer — Local Mode"""
import argparse
import logging
import os
import re
import sys
import shutil

if os.name == "nt":
    os.environ["HADOOP_HOME"] = os.getenv("HADOOP_HOME", "C:\\hadoop")
os.environ["SPARK_LOCAL_IP"]        = os.getenv("SPARK_LOCAL_IP", "127.0.0.1")
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from data_simulation.config import (
    BRONZE_ATM_MASTER, BRONZE_CARD_TRANSACTIONS, BRONZE_CARDS, BRONZE_CUSTOMERS,
    BRONZE_KAGGLE_TRANSACTIONS, BRONZE_OUT_OF_CASH, BRONZE_WALLET,
    LANDING_ATM_MASTER, LANDING_CARD_TRANSACTIONS, LANDING_CARDS, LANDING_CUSTOMERS,
    LANDING_KAGGLE_TRANSACTIONS, LANDING_OUT_OF_CASH, LANDING_WALLET,
    LOG_FORMAT, LOG_LEVEL, SPARK_APP_NAME, SPARK_LOG_LEVEL, LOCAL_BRONZE,
)

logging.basicConfig(format=LOG_FORMAT, level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("bronze_local")

def get_spark():
    try:
        spark = (
            SparkSession.builder
            .appName(f"{SPARK_APP_NAME}-bronze-local")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.local.dir", "/tmp/spark")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}", exc_info=True)
        raise

def clean_columns(df):
    new_cols = [re.sub(r'[ ,;{}()\n\t=]', '_', c).lower().strip('_') for c in df.columns]
    return df.toDF(*new_cols)

def reset_bronze():
    try:
        if os.path.exists(LOCAL_BRONZE):
            shutil.rmtree(LOCAL_BRONZE)
            logger.info(f"Deleted bronze: {LOCAL_BRONZE}")
        os.makedirs(LOCAL_BRONZE, exist_ok=True)
        logger.info("Bronze reset complete")
    except Exception as e:
        logger.error(f"Reset failed: {e}", exc_info=True)
        raise

def collect_parquet_files(landing_path):
    parquet_files = []
    if not os.path.exists(landing_path):
        logger.warning(f"Landing path does not exist: {landing_path}")
        return parquet_files
    for root, dirs, files in os.walk(landing_path):
        for f in files:
            if f.endswith(".parquet"):
                parquet_files.append(os.path.join(root, f))
    return parquet_files

def ingest_to_bronze(spark, landing_path, bronze_path, table_name, partition_by=None):
    try:
        if not os.path.exists(landing_path):
            logger.warning(f"Landing path does not exist: {landing_path}")
            return 0

        parquet_files = collect_parquet_files(landing_path)
        if not parquet_files:
            logger.warning(f"No parquet files in: {landing_path}")
            return 0

        logger.info(f"Ingesting {table_name} — {len(parquet_files)} parquet files")

        df = spark.read.option("mergeSchema", "true").parquet(*parquet_files)
        df = clean_columns(df)
        df = df.withColumn("_bronze_loaded_at", F.current_timestamp()).withColumn("_source_file", F.input_file_name())

        row_count = df.count()
        if row_count == 0:
            logger.warning(f"{table_name}: 0 rows — skipping")
            return 0

        logger.info(f"{table_name}: {row_count:,} rows")
        os.makedirs(bronze_path, exist_ok=True)

        writer = df.write.format("parquet").mode("overwrite").option("compression", "snappy")
        if partition_by and partition_by in df.columns:
            writer = writer.partitionBy(partition_by)
        writer.save(bronze_path)
        logger.info(f"✓ {table_name}: {row_count:,} rows → {bronze_path}")
        return row_count
    except Exception as e:
        logger.error(f"Ingestion failed for {table_name}: {e}", exc_info=True)
        raise

def bronze_health_check(spark, results):
    print("\n" + "="*70)
    print("BRONZE HEALTH CHECK")
    print("="*70)
    table_paths = {
        "raw_atm_master": BRONZE_ATM_MASTER,
        "raw_customers": BRONZE_CUSTOMERS,
        "raw_cards": BRONZE_CARDS,
        "raw_card_transactions": BRONZE_CARD_TRANSACTIONS,
        "raw_wallet_transactions": BRONZE_WALLET,
        "raw_out_of_cash": BRONZE_OUT_OF_CASH,
        "raw_kaggle_transactions": BRONZE_KAGGLE_TRANSACTIONS,
    }
    total_rows = 0
    for name, path in table_paths.items():
        try:
            if not os.path.exists(path):
                print(f"  {name:<35} NOT FOUND")
                continue
            df = spark.read.parquet(path)
            count = df.count()
            total_rows += count
            cols = len(df.columns)
            print(f"  {name:<35} {count:>10,} rows  {cols:>3} cols")
        except Exception as e:
            print(f"  {name:<35} ERROR: {e}")
    print("-"*70)
    print(f"  {'TOTAL':<35} {total_rows:>10,} rows")
    print("="*70)

def main(reset=False):
    try:
        logger.info("="*60)
        logger.info("Bronze Pipeline — LOCAL MODE")
        logger.info(f"Reset: {reset}")
        logger.info("="*60)

        if reset:
            reset_bronze()

        spark = get_spark()
        results = {}

        logger.info("── Ingesting dimensions ──")
        results["atm_master"] = ingest_to_bronze(spark, LANDING_ATM_MASTER, BRONZE_ATM_MASTER, "raw_atm_master")
        results["customers"] = ingest_to_bronze(spark, LANDING_CUSTOMERS, BRONZE_CUSTOMERS, "raw_customers")
        results["cards"] = ingest_to_bronze(spark, LANDING_CARDS, BRONZE_CARDS, "raw_cards")

        logger.info("── Ingesting facts ──")
        results["card_transactions"] = ingest_to_bronze(spark, LANDING_CARD_TRANSACTIONS, BRONZE_CARD_TRANSACTIONS, "raw_card_transactions", partition_by="transaction_date")
        results["wallet"] = ingest_to_bronze(spark, LANDING_WALLET, BRONZE_WALLET, "raw_wallet_transactions")
        results["out_of_cash"] = ingest_to_bronze(spark, LANDING_OUT_OF_CASH, BRONZE_OUT_OF_CASH, "raw_out_of_cash")
        results["kaggle_transactions"] = ingest_to_bronze(spark, LANDING_KAGGLE_TRANSACTIONS, BRONZE_KAGGLE_TRANSACTIONS, "raw_kaggle_transactions", partition_by="date")

        bronze_health_check(spark, results)
        logger.info("Bronze pipeline complete")
        logger.info(f"Summary: {results}")
        spark.stop()
        return 0

    except Exception as e:
        logger.error(f"Bronze pipeline failed: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Bronze ingestion — local mode")
        parser.add_argument("--reset", action="store_true", help="Delete and recreate all bronze tables")
        args = parser.parse_args()
        exit_code = main(reset=args.reset)
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
