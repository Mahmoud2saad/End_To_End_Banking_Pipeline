import duckdb
conn = duckdb.connect()
df = conn.execute("""SELECT * FROM read_parquet('D:/NTI INTERNSHIP/Airflow/Banking_pipeline/local_warehouse/delta/silver/atm_master/**/*.parquet') LIMIT 1""").df()
print('=== atm_master ===')
print(list(df.columns))
