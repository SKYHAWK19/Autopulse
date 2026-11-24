import os
import numpy as np
import pandas as pd


def generate_data(n_rows=100_000, output_path="data/raw_transactions.csv", seed=42):
    rng = np.random.default_rng(seed)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    txn_ids = np.array([f"TXN_{1000000 + i}" for i in range(n_rows)], dtype=object)
    # Inject ~2% duplicate transaction IDs
    dup_idx = rng.choice(n_rows, size=max(1, int(n_rows * 0.02)), replace=False)
    txn_ids[dup_idx] = txn_ids[rng.integers(0, n_rows, size=len(dup_idx))]

    vehicle_ids = rng.integers(101, 151, size=n_rows)
    base_ts = pd.date_range("2025-01-01", "2026-06-30", freq="min")
    timestamps = rng.choice(base_ts, size=n_rows).astype(str)
    bad_ts = rng.choice(n_rows, size=max(1, int(n_rows * 0.02)), replace=False)
    timestamps[bad_ts] = "INVALID_DATE"

    distances = np.round(rng.gamma(shape=3.0, scale=28.0, size=n_rows), 2).astype(object)
    distances[rng.choice(n_rows, size=max(1, int(n_rows * 0.03)), replace=False)] = np.nan

    currencies = rng.choice(["INR", "USD", "EUR", "GBP"], p=[0.74, 0.15, 0.09, 0.02], size=n_rows)
    txn_types = rng.choice(["FUEL", "TOLL", "MAINTENANCE", "REFUND"], p=[0.55, 0.25, 0.16, 0.04], size=n_rows)
    amounts = np.round(rng.uniform(15.0, 450.0, size=n_rows), 2)
    amounts[txn_types == "REFUND"] *= -1
    # Inject ~1.5% extreme outlier spikes
    spike_idx = rng.choice(n_rows, size=max(1, int(n_rows * 0.015)), replace=False)
    amounts[spike_idx] *= 15.0

    df = pd.DataFrame(
        {
            "Transaction ID": txn_ids,
            "Vehicle ID": vehicle_ids,
            "Timestamp": timestamps,
            "Distance KM": distances,
            "Amount": amounts,
            "Currency": currencies,
            "Txn Type": txn_types,
        }
    )
    df.to_csv(output_path, index=False)
    return output_path