import os
import pandas as pd
import mysql.connector
from dotenv import load_dotenv

load_dotenv()


def load_to_mysql(csv_path="data/clean_transactions.csv", chunk_size=10_000):
    conn = mysql.connector.connect(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "root"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "autopulse_db"),
    )
    cur = conn.cursor()

    insert_sql = """
    INSERT IGNORE INTO trip_logs
        (transaction_id, vehicle_id, trip_date, distance_km, amount_inr, txn_type, is_outlier)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    total_attempted = 0
    total_inserted = 0

    for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
        unique_vids = sorted(chunk["vehicle_id"].dropna().astype(int).unique())
        # Deterministic synthetic metadata for demo purposes
        vehicles = [(int(v), f"Fleet-Unit-{v}", "Diesel" if v % 2 else "CNG") for v in unique_vids]
        cur.executemany(
            "INSERT IGNORE INTO vehicles (vehicle_id, model_name, fuel_type) VALUES (%s, %s, %s)",
            vehicles,
        )

        rows = list(
            zip(
                chunk["transaction_id"].astype(str),
                chunk["vehicle_id"].astype(int),
                chunk["timestamp"],
                chunk["distance_km"],
                chunk["amount_inr"],
                chunk["txn_type"],
                chunk["is_outlier"].astype(int),
            )
        )
        cur.executemany(insert_sql, rows)
        conn.commit()
        total_attempted += len(rows)
        total_inserted += cur.rowcount

    print(f"Processed {total_attempted:,} rows | Newly inserted: {total_inserted:,} (duplicates skipped)")
    cur.close()
    conn.close()


if __name__ == "__main__":
    load_to_mysql()