from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, lower, upper, current_timestamp, to_date, to_timestamp,
    when, regexp_replace, coalesce, input_file_name
)

spark = SparkSession.builder.appName("banking_silver_local").getOrCreate()

BASE_DIR = Path("D:/NTI INTERNSHIP/Airflow/banking-pipeline")
BRONZE_DIR = BASE_DIR / "local_warehouse" / "delta" / "bronze"
SILVER_DIR = BASE_DIR / "local_warehouse" / "delta" / "silver"

def read_bronze(name):
    return spark.read.format("delta").load(str(BRONZE_DIR / name))

def write_silver(df, name):
    target = SILVER_DIR / name
    df.write.format("delta").mode("overwrite").save(str(target))
    print(f"loaded silver: {name} -> {target}")

def standardize_common(df):
    cols = df.columns
    for c in cols:
        df = df.withColumnRenamed(c, c.strip())
    return df

def add_common_metadata(df):
    return (
        df.withColumn("_silver_processed_at", current_timestamp())
          .withColumn("_silver_source_file", input_file_name())
    )

# users_data
if (BRONZE_DIR / "users_data").exists():
    df = standardize_common(read_bronze("users_data"))
    if "customer_id" in df.columns:
        df = df.withColumn("customer_id", trim(col("customer_id")))
    if "income" in df.columns:
        df = df.withColumn("income_mad", col("income").cast("double"))
    if "city" in df.columns:
        df = df.withColumn("city", trim(col("city")))
    if "phone" in df.columns:
        df = df.withColumn("phone", regexp_replace(col("phone"), r"[^0-9+]", ""))
    df = add_common_metadata(df)
    write_silver(df, "users_data")

# cards_data
if (BRONZE_DIR / "cards_data").exists():
    df = standardize_common(read_bronze("cards_data"))
    if "card_id" in df.columns:
        df = df.withColumn("card_id", trim(col("card_id")))
    if "customer_id" in df.columns:
        df = df.withColumn("customer_id", trim(col("customer_id")))
    if "credit_limit" in df.columns:
        df = df.withColumn("credit_limit_mad", col("credit_limit").cast("double"))
    if "card_status" in df.columns:
        df = df.withColumn("card_status", upper(trim(col("card_status"))))
    df = add_common_metadata(df)
    write_silver(df, "cards_data")

# atm_master
if (BRONZE_DIR / "atm_master").exists():
    df = standardize_common(read_bronze("atm_master"))
    if "atm_id" in df.columns:
        df = df.withColumn("atm_id", trim(col("atm_id")))
    if "region" in df.columns:
        df = df.withColumn("region", trim(col("region")))
    if "daily_limit" in df.columns:
        df = df.withColumn("daily_limit_mad", col("daily_limit").cast("double"))
    df = add_common_metadata(df)
    write_silver(df, "atm_master")

# transactions_data
if (BRONZE_DIR / "transactions_data").exists():
    df = standardize_common(read_bronze("transactions_data"))
    rename_map = {
        "txn_id": "transaction_id",
        "cust_id": "customer_id",
        "card_no": "card_id",
        "amt": "amount",
        "txn_date": "transaction_date",
        "txn_time": "transaction_time",
        "merchant_cat": "merchant_category"
    }
    for old, new in rename_map.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)

    if "amount" in df.columns:
        df = df.withColumn("amount_mad", col("amount").cast("double"))
    if "transaction_date" in df.columns:
        df = df.withColumn("transaction_date", to_date(col("transaction_date")))
    if "transaction_time" in df.columns:
        df = df.withColumn("transaction_time", trim(col("transaction_time")))
    if "city" in df.columns:
        df = df.withColumn("city", trim(col("city")))
    if "fraud_label" in df.columns:
        df = df.withColumn("is_fraudulent", col("fraud_label").cast("int"))
    if "channel" in df.columns:
        df = df.withColumn("channel", upper(trim(col("channel"))))
    df = add_common_metadata(df)
    write_silver(df, "transactions_data")

# wallet
if (BRONZE_DIR / "wallet").exists():
    df = standardize_common(read_bronze("wallet"))
    if "wallet_id" in df.columns:
        df = df.withColumn("wallet_id", trim(col("wallet_id")))
    if "amount" in df.columns:
        df = df.withColumn("amount_mad", col("amount").cast("double"))
    if "transaction_date" in df.columns:
        df = df.withColumn("transaction_date", to_date(col("transaction_date")))
    df = add_common_metadata(df)
    write_silver(df, "wallet")

# out_of_cash
if (BRONZE_DIR / "out_of_cash").exists():
    df = standardize_common(read_bronze("out_of_cash"))
    if "atm_id" in df.columns:
        df = df.withColumn("atm_id", trim(col("atm_id")))
    if "event_date" in df.columns:
        df = df.withColumn("event_date", to_date(col("event_date")))
    if "city" in df.columns:
        df = df.withColumn("city", trim(col("city")))
    df = add_common_metadata(df)
    write_silver(df, "out_of_cash")

# mcc_codes
if (BRONZE_DIR / "mcc_codes").exists():
    df = standardize_common(read_bronze("mcc_codes"))
    if "mcc" in df.columns:
        df = df.withColumn("mcc", trim(col("mcc")))
    if "category" in df.columns:
        df = df.withColumn("category", trim(col("category")))
    df = add_common_metadata(df)
    write_silver(df, "mcc_codes")

# train_fraud_labels
if (BRONZE_DIR / "train_fraud_labels").exists():
    df = standardize_common(read_bronze("train_fraud_labels"))
    if "transaction_id" in df.columns:
        df = df.withColumn("transaction_id", trim(col("transaction_id")))
    if "fraud_label" in df.columns:
        df = df.withColumn("fraud_label", col("fraud_label").cast("int"))
    df = add_common_metadata(df)
    write_silver(df, "train_fraud_labels")

spark.stop()
