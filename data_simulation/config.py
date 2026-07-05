# data_simulation/config.py
"""
Central configuration for the Banking Pipeline.
All paths, settings, and constants live here.
Every other file imports from this module.
Supports both LOCAL and CLOUD (Azure) environments
via the ENV environment variable.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Environment ───────────────────────────────────────────────────────────────
# Accepts the dev|staging|prod vocabulary (docs/SECURITY_AND_GOVERNANCE.md
# section 4) as well as the original local|cloud values, for backward
# compatibility with any existing scripts/DAGs still passing ENV=local.
ENV = os.getenv("ENV", "dev")
IS_LOCAL = ENV in ("dev", "local")
IS_CLOUD = ENV in ("staging", "prod", "cloud")
if not (IS_LOCAL or IS_CLOUD):
    raise ValueError(
        f"Unrecognized ENV='{ENV}'. Expected one of: dev, staging, prod, local, cloud. "
        f"See docs/SECURITY_AND_GOVERNANCE.md section 4."
    )

# ── Azure Storage (cloud only) ────────────────────────────────────────────────
STORAGE_ACCOUNT    = os.getenv("AZURE_STORAGE_ACCOUNT_NAME", "bankingpipelinesa")
ABFSS_LANDING      = f"abfss://landing@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ABFSS_BRONZE       = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ABFSS_SILVER       = f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ABFSS_GOLD         = f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ── Local paths ───────────────────────────────────────────────────────────────
BASE_DIR           = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_SYNTHETIC     = os.path.join(BASE_DIR, "data", "synthetic")
DATA_KAGGLE        = os.path.join(BASE_DIR, "data", "kaggle")
LOCAL_LANDING      = os.path.join(BASE_DIR, "local_warehouse", "delta", "landing")
LOCAL_BRONZE       = os.path.join(BASE_DIR, "local_warehouse", "delta", "bronze")
LOCAL_SILVER       = os.path.join(BASE_DIR, "local_warehouse", "delta", "silver")
LOCAL_GOLD         = os.path.join(BASE_DIR, "local_warehouse", "delta", "gold")

# ── Source data files ─────────────────────────────────────────────────────────
ATM_MASTER_FILE         = os.path.join(DATA_SYNTHETIC, "atm_master.csv")
CARDS_TXN_FILE          = os.path.join(DATA_SYNTHETIC, "cards.csv")
WALLET_FILE             = os.path.join(DATA_SYNTHETIC, "wallet.csv")
OUT_OF_CASH_FILE        = os.path.join(DATA_SYNTHETIC, "out_of_cash.csv")
USERS_FILE              = os.path.join(DATA_SYNTHETIC, "users_data.csv")
CARDS_FILE              = os.path.join(DATA_SYNTHETIC, "cards_data.csv")
MCC_CODES_FILE          = os.path.join(DATA_SYNTHETIC, "mcc_codes.json")
FRAUD_LABELS_FILE       = os.path.join(DATA_SYNTHETIC, "train_fraud_labels.json")
ATM_MAP_FILE            = os.path.join(DATA_SYNTHETIC, "atm_governorate_region_map.json")
KAGGLE_TRANSACTIONS     = os.path.join(DATA_KAGGLE,    "transactions_data.csv")

# ── Dynamic path resolver ─────────────────────────────────────────────────────
def get_path(layer: str, table: str) -> str:
    """
    Returns the correct path for a given layer and table
    based on current environment (local or cloud).

    Usage:
        get_path("bronze", "raw_card_transactions")
        -> local:  /local_warehouse/delta/bronze/raw_card_transactions
        -> cloud:  abfss://bronze@bankingpipelinesa.dfs.core.windows.net/raw_card_transactions
    """
    if IS_LOCAL:
        base = {
            "landing": LOCAL_LANDING,
            "bronze":  LOCAL_BRONZE,
            "silver":  LOCAL_SILVER,
            "gold":    LOCAL_GOLD,
        }[layer]
        return os.path.join(base, table)
    else:
        base = {
            "landing": ABFSS_LANDING,
            "bronze":  ABFSS_BRONZE,
            "silver":  ABFSS_SILVER,
            "gold":    ABFSS_GOLD,
        }[layer]
        return f"{base}/{table}"

# ── Landing paths ─────────────────────────────────────────────────────────────
LANDING_CARD_TRANSACTIONS   = get_path("landing", "card_transactions")
LANDING_ATM_TRANSACTIONS    = get_path("landing", "atm_transactions")
LANDING_WALLET              = get_path("landing", "wallet_transactions")
LANDING_OUT_OF_CASH         = get_path("landing", "out_of_cash")
LANDING_CUSTOMERS           = get_path("landing", "customers")
LANDING_CARDS               = get_path("landing", "cards")
LANDING_ATM_MASTER          = get_path("landing", "atm_master")
LANDING_KAGGLE_TRANSACTIONS = get_path("landing", "kaggle_transactions")

# ── Bronze paths ──────────────────────────────────────────────────────────────
BRONZE_CARD_TRANSACTIONS    = get_path("bronze", "raw_card_transactions")
BRONZE_ATM_TRANSACTIONS     = get_path("bronze", "raw_atm_transactions")
BRONZE_WALLET               = get_path("bronze", "raw_wallet_transactions")
BRONZE_OUT_OF_CASH          = get_path("bronze", "raw_out_of_cash")
BRONZE_CUSTOMERS            = get_path("bronze", "raw_customers")
BRONZE_CARDS                = get_path("bronze", "raw_cards")
BRONZE_ATM_MASTER           = get_path("bronze", "raw_atm_master")
BRONZE_KAGGLE_TRANSACTIONS  = get_path("bronze", "raw_kaggle_transactions")

# ── Silver paths ──────────────────────────────────────────────────────────────
SILVER_CARD_TRANSACTIONS    = get_path("silver", "card_transactions")
SILVER_ATM_TRANSACTIONS     = get_path("silver", "atm_transactions")
SILVER_WALLET               = get_path("silver", "wallet_transactions")
SILVER_OUT_OF_CASH          = get_path("silver", "out_of_cash_events")
SILVER_CUSTOMERS            = get_path("silver", "customers")
SILVER_CARDS                = get_path("silver", "cards")
SILVER_ATM_MASTER           = get_path("silver", "atm_master")
SILVER_KAGGLE_TRANSACTIONS  = get_path("silver", "kaggle_transactions")

# ── Gold paths ────────────────────────────────────────────────────────────────
GOLD_FACT_CARD_TRANSACTIONS = get_path("gold", "fact_card_transactions")
GOLD_FACT_ATM_TRANSACTIONS  = get_path("gold", "fact_atm_transactions")
GOLD_FACT_WALLET            = get_path("gold", "fact_wallet_transactions")
GOLD_FACT_OOC               = get_path("gold", "fact_out_of_cash_events")
GOLD_DIM_CUSTOMER           = get_path("gold", "dim_customer")
GOLD_DIM_CARD               = get_path("gold", "dim_card")
GOLD_DIM_ATM                = get_path("gold", "dim_atm")
GOLD_DIM_DATE               = get_path("gold", "dim_date")
GOLD_DIM_MERCHANT           = get_path("gold", "dim_merchant")
GOLD_DIM_MCC                = get_path("gold", "dim_merchant_category")
GOLD_DIM_CHANNEL            = get_path("gold", "dim_channel")
GOLD_DIM_ERROR              = get_path("gold", "dim_error_type")

# ── Checkpoints (cloud only) ──────────────────────────────────────────────────
CHECKPOINT_CARD_TRANSACTIONS   = get_path("bronze", "_checkpoints/card_transactions")
CHECKPOINT_ATM_TRANSACTIONS    = get_path("bronze", "_checkpoints/atm_transactions")
CHECKPOINT_WALLET              = get_path("bronze", "_checkpoints/wallet")
CHECKPOINT_OUT_OF_CASH         = get_path("bronze", "_checkpoints/out_of_cash")
CHECKPOINT_CUSTOMERS           = get_path("bronze", "_checkpoints/customers")
CHECKPOINT_CARDS               = get_path("bronze", "_checkpoints/cards")
CHECKPOINT_ATM_MASTER          = get_path("bronze", "_checkpoints/atm_master")
CHECKPOINT_KAGGLE_TRANSACTIONS = get_path("bronze", "_checkpoints/kaggle_transactions")

# ── Quarantine ────────────────────────────────────────────────────────────────
QUARANTINE_PATH = get_path("silver", "_quarantine")

# ── Simulation settings ───────────────────────────────────────────────────────
SIMULATOR_BATCH_SIZE  = int(os.getenv("SIMULATOR_BATCH_SIZE", 50))
SIMULATOR_DELAY_SECS  = float(os.getenv("SIMULATOR_DELAY_SECS", 5.0))
SIMULATOR_MAX_BATCHES = os.getenv("SIMULATOR_MAX_BATCHES", None)

# ── Spark settings ────────────────────────────────────────────────────────────
SPARK_APP_NAME   = "banking-pipeline"
SPARK_MASTER     = os.getenv("SPARK_MASTER", "local[*]")
SPARK_LOG_LEVEL  = os.getenv("SPARK_LOG_LEVEL", "WARN")

# ── dbt settings ─────────────────────────────────────────────────────────────
DBT_PROJECT_DIR  = os.path.join(BASE_DIR, "banking_dbt")
DBT_TARGET       = os.getenv("DBT_TARGET", "dev")

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_LEVEL  = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"

# ── Business constants ────────────────────────────────────────────────────────
BANK_NAME        = "Attijariwafa Bank"
CURRENCY         = "MAD"
COUNTRY          = "Maroc"
MAX_ATM_AMOUNT   = 4000    # MAD — Moroccan ATM withdrawal limit
MIN_ATM_AMOUNT   = 100     # MAD — minimum transaction
FRAUD_THRESHOLD  = 0.02    # 2% fraud rate threshold for HIGH risk

# ── Validation ────────────────────────────────────────────────────────────────
VALID_RESP_CODES    = [0, 96, 51, 14, 61, 43, 54]
VALID_MSG_TYPES     = [200, 210, 420, 430]
VALID_ATM_TYPES     = ["Retrait & Dépôt", "Retrait uniquement"]
VALID_CARD_TYPES    = ["Débit", "Crédit"]
VALID_CARD_BRANDS   = ["Visa", "Mastercard", "CMI"]
VALID_SEGMENTS      = ["PREMIUM", "STANDARD", "BASIC"]
VALID_WALLET_TYPES  = ["DEMANDE RETRAIT", "DEMANDE DÉPÔT", "ANNULATION RETRAIT"]

if __name__ == "__main__":
    print(f"ENV:          {ENV}")
    print(f"BANK:         {BANK_NAME}")
    print(f"CURRENCY:     {CURRENCY}")
    print(f"BRONZE cards: {BRONZE_CARD_TRANSACTIONS}")
    print(f"SILVER cards: {SILVER_CARD_TRANSACTIONS}")
    print("Config loaded successfully")