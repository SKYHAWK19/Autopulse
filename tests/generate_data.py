import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def generate_data(n_rows=105_000, seed=None):
    rng = np.random.default_rng(seed)
    base_date = datetime(2025, 1, 1)
    timestamps = [base_date + timedelta(minutes=int(x)) for x in rng.integers(0, 525600, size=n_rows)]

    df = pd.DataFrame({
        "transaction_id": [f"TXN-{100000 + i}" for i in range(n_rows)],
        "vehicle_id": rng.integers(1, 51, size=n_rows),
        "timestamp": timestamps,
        "distance_km": np.round(rng.lognormal(mean=3.2, sigma=0.8, size=n_rows), 2),
        "amount": np.round(rng.normal(loc=2400, scale=850, size=n_rows), 2),
        "currency": rng.choice(["INR", "USD", "EUR", "inr ", None], size=n_rows, p=[0.88, 0.04, 0.02, 0.04, 0.02]),
        "txn_type": rng.choice(["FUEL", "MAINTENANCE", "TOLL", "REFUND"], size=n_rows, p=[0.65, 0.20, 0.12, 0.03])
    })

    # Inject errors: duplicates, nulls, negative charges, outliers
    dup_idx = rng.choice(n_rows, size=int(n_rows * 0.025), replace=False)
    df.loc[dup_idx, "transaction_id"] = df.loc[rng.choice(n_rows, size=len(dup_idx)), "transaction_id"].values
    df.loc[rng.choice(n_rows, size=int(n_rows * 0.02), replace=False), "timestamp"] = pd.NaT
    df.loc[rng.choice(n_rows, size=int(n_rows * 0.015), replace=False), "distance_km"] = np.nan

    neg_idx = rng.choice(df[df["txn_type"] != "REFUND"].index, size=int(n_rows * 0.01), replace=False)
    df.loc[neg_idx, "amount"] = -np.abs(df.loc[neg_idx, "amount"])
    df.loc[rng.choice(n_rows, size=250, replace=False), "amount"] = rng.uniform(85000, 250000, size=250)

    df.to_csv("data/raw_transactions.csv", index=False)
    print(f"Generated {n_rows} messy rows in data/raw_transactions.csv")

if __name__ == "__main__":
    generate_data()