import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

conn = mysql.connector.connect(
    host=os.getenv("DB_HOST", "localhost"),
    user=os.getenv("DB_USER", "root"),
    password=os.getenv("DB_PASSWORD", ""),
    database=os.getenv("DB_NAME", "autopulse_db"),
)
cur = conn.cursor()

df = pd.read_csv("data/clean_transactions.csv")

# 1. Ensure all vehicle_ids exist in vehicles master table (FK safe)
unique_vids = sorted(df["vehicle_id"].dropna().astype(int).unique())
vehicles = [(int(v), f"Fleet-Unit-{v}", "Diesel" if v % 2 else "CNG") for v in unique_vids]
cur.executemany(
    "INSERT IGNORE INTO vehicles (vehicle_id, model_name, fuel_type) VALUES (%s, %s, %s)",
    vehicles,
)
conn.commit()

# 2. Batch-insert cleaned telemetry into trip_logs
rows = list(
    zip(
        df["vehicle_id"].astype(int),
        df["timestamp"],
        df["distance_km"],
        df["amount_inr"],
        df["txn_type"],
        df["is_outlier"].astype(int),
    )
)

insert_sql = """
INSERT INTO trip_logs (vehicle_id, trip_date, distance_km, amount_inr, txn_type, is_outlier)
VALUES (%s, %s, %s, %s, %s, %s)
"""
for i in range(0, len(rows), 10000):
    cur.executemany(insert_sql, rows[i : i + 10000])
    conn.commit()

print(f"Inserted {len(rows):,} rows into autopulse_db.trip_logs")
cur.close()
conn.close()