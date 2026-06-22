from pathlib import Path
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit, to_date, col

spark = SparkSession.builder.appName("banking_bronze_local").getOrCreate()

BASE_DIR = Path("actual data path")
RAW_DIR = BASE_DIR / "data" 
BRONZE_DIR = BASE_DIR / "local_warehouse" / "delta" / "bronze"

sources = {
    "atm_master": {"path": RAW_DIR / "atm_master.csv", "format": "csv"},
    "cards": {"path": RAW_DIR / "cards.csv", "format": "csv"},
    "wallet": {"path": RAW_DIR / "wallet.csv", "format": "csv"},
    "out_of_cash": {"path": RAW_DIR / "out_of_cash.csv", "format": "csv"},
    "users_data": {"path": RAW_DIR / "users_data.csv", "format": "csv"},
    "cards_data": {"path": RAW_DIR / "cards_data.csv", "format": "csv"},
    "transactions_data": {"path": RAW_DIR / "transactions_data.csv", "format": "csv"},
    "mcc_codes": {"path": RAW_DIR / "mcc_codes.json", "format": "json"},
    "train_fraud_labels": {"path": RAW_DIR / "train_fraud_labels.json", "format": "json"},
}

def read_source(path, fmt):
    if fmt == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(str(path))
    return spark.read.option("multiline", True).json(str(path))

for name, meta in sources.items():
    if not meta["path"].exists():
        print(f"skipping missing source: {name} -> {meta['path']}")
        continue

    df = read_source(meta["path"], meta["format"])

    df_bronze = (
        df
        .withColumn("_ingestion_ts", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_source_system", lit(name))
        .withColumn("_bronze_loaded_at", current_timestamp())
    )

    target_path = BRONZE_DIR / name
    df_bronze.write.format("delta").mode("overwrite").save(str(target_path))

    print(f"loaded bronze: {name} -> {target_path}")

spark.stop()
