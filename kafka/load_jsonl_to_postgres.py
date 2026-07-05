import json
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values

JSONL_FILE = Path(r"D:\NTI INTERNSHIP\Airflow\Banking_pipeline\data\bronze\transactions\events.jsonl")

PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "your_database"
PG_USER = "postgres"
PG_PASSWORD = "your_password"

TABLE_NAME = "bronze_transactions_raw"

def ensure_table(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            trans_id TEXT,
            branch_code TEXT,
            acc_no TEXT,
            transtype_id TEXT,
            trans_postdate TIMESTAMP,
            trans_desc TEXT,
            trans_amount NUMERIC(18,2),
            loaded_at TIMESTAMP DEFAULT NOW()
        );
    """)

def parse_row(obj):
    return (
        obj.get("trans_id"),
        obj.get("branch_code"),
        obj.get("acc_no"),
        obj.get("transtype_id"),
        obj.get("trans_postdate"),
        obj.get("trans_desc"),
        obj.get("trans_amount"),
    )

def main():
    rows = []
    with JSONL_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                obj = json.loads(line)
                rows.append(parse_row(obj))

    if not rows:
        print("No rows found.")
        return

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            execute_values(
                cur,
                f"""
                INSERT INTO {TABLE_NAME}
                (trans_id, branch_code, acc_no, transtype_id, trans_postdate, trans_desc, trans_amount)
                VALUES %s
                """,
                rows
            )
        conn.commit()
        print(f"Loaded {len(rows)} rows into {TABLE_NAME}")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    main()
