import duckdb
conn = duckdb.connect()

print('=== card_transactions ===')
df = conn.execute("""SELECT * FROM read_parquet('D:/NTI INTERNSHIP/Airflow/Banking_pipeline/local_warehouse/delta/silver/card_transactions/**/*.parquet') LIMIT 1""").df()
print(list(df.columns))

print()
print('=== out_of_cash ===')
df2 = conn.execute("""SELECT * FROM read_parquet('D:/NTI INTERNSHIP/Airflow/Banking_pipeline/local_warehouse/delta/silver/out_of_cash/**/*.parquet') LIMIT 1""").df()
print(list(df2.columns))
