import json
import re
import time
from collections import defaultdict
from datetime import datetime
import numpy as np
import pandas as pd

FX_RATES = {"INR": 1.0, "USD": 84.0, "EUR": 91.5}
CHUNK_SIZE = 100_000
NUM_PATTERN = re.compile(r"^[\s₹$€£,+-]*\d+(\.\d+)?[\s]*$")

MISSING_TOKENS = {"", "nan", "NaN", "None", "none", "null", "NULL", "N/A", "n/a", "NA", "na", "-"}
ERROR_TOKENS = {"ERROR", "error", "#REF!", "#VALUE!", "#DIV/0!", "#N/A", "INVALID", "invalid"}
UNKNOWN_TOKENS = {"UNKNOWN", "unknown"}
ALL_DIRTY_TOKENS = list(MISSING_TOKENS | ERROR_TOKENS | UNKNOWN_TOKENS)


def clean_col_name(col):
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")


def coerce_dirty_numeric(series):
    """Strips dirty placeholders and currency symbols, converting numeric strings to float64."""
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    cleaned_str = series.astype(str).str.strip().replace(ALL_DIRTY_TOKENS, np.nan)
    non_null = cleaned_str.dropna()
    if non_null.empty:
        return series

    sample = non_null.head(500)
    if sample.str.match(NUM_PATTERN).mean() >= 0.75:
        numeric_str = cleaned_str.str.replace(r"[₹$€£,\s]", "", regex=True)
        return pd.to_numeric(numeric_str, errors="coerce")
    return series


def detect_math_triplet(numeric_cols):
    """Detects (quantity, unit_price, total_amount) column triplets dynamically."""
    qty_col = next((c for c in numeric_cols if any(k in c for k in ["qty", "quantity", "units", "count"])), None)
    price_col = next((c for c in numeric_cols if any(k in c for k in ["price", "rate", "unit_cost", "cost_per"])), None)
    total_col = next((c for c in numeric_cols if any(k in c for k in ["total", "amount", "revenue", "sales", "spend"])), None)
    if qty_col and price_col and total_col and len({qty_col, price_col, total_col}) == 3:
        return (qty_col, price_col, total_col)
    return None


def profile_schema(input_path):
    """Pass 1: Auto-detects column types, lookup mappings, global medians, and Tukey 1.5x IQR fences."""
    sample = pd.read_csv(input_path, nrows=2000)
    raw_cols = list(sample.columns)
    norm_cols = [clean_col_name(c) for c in raw_cols]
    col_map = dict(zip(raw_cols, norm_cols))
    sample = sample.rename(columns=col_map)

    is_fleet = {"transaction_id", "vehicle_id", "amount", "currency"}.issubset(set(norm_cols))
    id_cols = [c for c in norm_cols if c == "id" or c.endswith("_id")]
    date_cols = [c for c in norm_cols if any(k in c for k in ["date", "time", "timestamp", "created"])]

    numeric_cols = []
    cat_cols = []
    for c in norm_cols:
        if c in id_cols or c in date_cols:
            continue
        converted = coerce_dirty_numeric(sample[c])
        if pd.api.types.is_numeric_dtype(converted):
            numeric_cols.append(c)
        else:
            cat_cols.append(c)

    # Learn deterministic categorical -> numeric lookup maps (e.g., item -> price_per_unit)
    lookup_maps = {}
    for cc in cat_cols:
        clean_cat = sample[cc].astype(str).str.strip()
        valid_cat = ~clean_cat.isin(ALL_DIRTY_TOKENS) & sample[cc].notna()
        if valid_cat.sum() < 10 or clean_cat[valid_cat].nunique() > 200:
            continue
        for nc in numeric_cols:
            num_s = coerce_dirty_numeric(sample[nc])
            both_valid = valid_cat & num_s.notna()
            if both_valid.sum() < 10:
                continue
            grp_std = num_s[both_valid].groupby(clean_cat[both_valid]).std().dropna()
            if not grp_std.empty and (grp_std <= 0.01).mean() >= 0.90:
                mapping = num_s[both_valid].groupby(clean_cat[both_valid]).median().to_dict()
                lookup_maps[(cc, nc)] = mapping

    num_values = defaultdict(list)
    for chunk in pd.read_csv(input_path, chunksize=CHUNK_SIZE):
        chunk = chunk.rename(columns=col_map)
        for c in numeric_cols:
            s = coerce_dirty_numeric(chunk[c]).dropna()
            if not s.empty:
                num_values[c].append(s.to_numpy(dtype="float64"))

    stats = {}
    for c in numeric_cols:
        if not num_values[c]:
            continue
        arr = np.concatenate(num_values[c])
        q1, med, q3 = np.percentile(arr, [25, 50, 75])
        iqr = q3 - q1
        stats[c] = {
            "median": float(med),
            "upper_fence": float(q3 + 1.5 * iqr),
        }

    return {
        "col_map": col_map,
        "is_fleet": is_fleet,
        "id_cols": id_cols,
        "date_cols": date_cols,
        "numeric_cols": list(stats.keys()),
        "cat_cols": cat_cols,
        "lookup_maps": lookup_maps,
        "math_triplet": detect_math_triplet(list(stats.keys())),
        "stats": stats,
    }


def normalize_text_series(series):
    """Standardizes text while preserving distinction between MISSING, INVALID_ENTRY, and UNKNOWN."""
    res = series.fillna("MISSING").astype(str).str.strip()
    res = res.replace({tok: "MISSING" for tok in MISSING_TOKENS})
    res = res.replace({tok: "INVALID_ENTRY" for tok in ERROR_TOKENS})
    res = res.replace({tok: "UNKNOWN" for tok in UNKNOWN_TOKENS})
    return res


def run_etl(input_path="data/raw_transactions.csv", output_path="data/clean_transactions.csv"):
    start_time = time.perf_counter()
    schema = profile_schema(input_path)

    seen_ids = set()
    initial_rows = 0
    clean_rows = 0
    imputed_cells = 0
    missing_dates_flagged = 0
    outliers_flagged = 0
    drop_log = {"duplicates": 0, "null_dates_or_empty": 0, "invalid_values": 0}
    first_chunk = True

    pk_col = schema["id_cols"][0] if schema["id_cols"] else None

    for chunk in pd.read_csv(input_path, chunksize=CHUNK_SIZE):
        initial_rows += len(chunk)
        chunk = chunk.rename(columns=schema["col_map"])

        # 1. Drop completely blank rows & deduplicate
        before = len(chunk)
        chunk = chunk.dropna(how="all")
        if pk_col and pk_col in chunk.columns:
            chunk = chunk.dropna(subset=[pk_col]).drop_duplicates(subset=[pk_col], keep="first")
            is_new = ~chunk[pk_col].astype(str).isin(seen_ids)
            seen_ids.update(chunk.loc[is_new, pk_col].astype(str))
            chunk = chunk[is_new].copy()
        else:
            chunk = chunk.drop_duplicates().copy()
        drop_log["duplicates"] += before - len(chunk)

        # 2. Parse detected date/timestamp columns (kept separate from data imputation flags)
        if schema["is_fleet"]:
            for dc in schema["date_cols"]:
                before = len(chunk)
                chunk[dc] = pd.to_datetime(chunk[dc], errors="coerce")
                chunk = chunk.dropna(subset=[dc]).copy()
                drop_log["null_dates_or_empty"] += before - len(chunk)
        elif schema["date_cols"]:
            date_missing_mask = np.zeros(len(chunk), dtype=bool)
            for dc in schema["date_cols"]:
                parsed_dt = pd.to_datetime(chunk[dc], errors="coerce")
                is_bad = parsed_dt.isna()
                date_missing_mask |= is_bad.to_numpy()
                chunk[dc] = parsed_dt.dt.strftime("%Y-%m-%d").fillna("MISSING")
            chunk["is_date_missing"] = date_missing_mask
            missing_dates_flagged += int(date_missing_mask.sum())

        # 3. Fleet-specific FX & negative charge rules (runs only if fleet schema)
        if schema["is_fleet"]:
            before = len(chunk)
            chunk["currency"] = chunk["currency"].fillna("UNKNOWN").astype(str).str.strip().str.upper()
            chunk = chunk[chunk["currency"].isin(FX_RATES.keys())].copy()
            drop_log["invalid_values"] += before - len(chunk)
            chunk["amount_inr"] = np.round(pd.to_numeric(chunk["amount"], errors="coerce") * chunk["currency"].map(FX_RATES), 2)

            bad_neg = (chunk["amount_inr"] <= 0) & (chunk["txn_type"].astype(str).str.upper() != "REFUND")
            drop_log["invalid_values"] += int(bad_neg.sum())
            chunk = chunk[~bad_neg].copy()

        # Track per-column cell modifications accurately
        col_imputed_masks = {}

        # 4. Universal numeric coercion, lookup recovery & algebraic reconciliation
        for nc in schema["numeric_cols"]:
            if nc not in chunk.columns:
                continue
            chunk[nc] = coerce_dirty_numeric(chunk[nc])
            col_imputed_masks[nc] = chunk[nc].isna().to_numpy().copy()

        triplet = schema["math_triplet"]
        if triplet:
            q_col, p_col, t_col = triplet
            # Pass A: Solve missing 3rd variable when 2 are present
            m_tot = chunk[t_col].isna() & chunk[q_col].notna() & chunk[p_col].notna()
            chunk.loc[m_tot, t_col] = chunk.loc[m_tot, q_col] * chunk.loc[m_tot, p_col]

            m_qty = chunk[q_col].isna() & chunk[t_col].notna() & (chunk[p_col] > 0)
            chunk.loc[m_qty, q_col] = (chunk.loc[m_qty, t_col] / chunk.loc[m_qty, p_col]).round(0)

            m_prc = chunk[p_col].isna() & chunk[t_col].notna() & (chunk[q_col] > 0)
            chunk.loc[m_prc, p_col] = (chunk.loc[m_prc, t_col] / chunk.loc[m_prc, q_col]).round(2)

        # Pass B: Apply learned lookup maps (e.g., item -> price_per_unit) before median fallback
        for (cc, nc), mapping in schema["lookup_maps"].items():
            if cc in chunk.columns and nc in chunk.columns:
                miss_mask = chunk[nc].isna()
                if miss_mask.any():
                    recovered = chunk.loc[miss_mask, cc].astype(str).str.strip().map(mapping)
                    chunk.loc[miss_mask, nc] = recovered

        if triplet:
            q_col, p_col, t_col = triplet
            # Pass C: Re-run algebraic solver using newly recovered unit prices
            m_tot = chunk[t_col].isna() & chunk[q_col].notna() & chunk[p_col].notna()
            chunk.loc[m_tot, t_col] = chunk.loc[m_tot, q_col] * chunk.loc[m_tot, p_col]

            m_qty = chunk[q_col].isna() & chunk[t_col].notna() & (chunk[p_col] > 0)
            chunk.loc[m_qty, q_col] = (chunk.loc[m_qty, t_col] / chunk.loc[m_qty, p_col]).round(0)

        # Pass D: Median fallback for remaining nulls
        for nc in schema["numeric_cols"]:
            if nc not in chunk.columns:
                continue
            chunk[nc] = chunk[nc].fillna(schema["stats"][nc]["median"]).round(2)

        # Pass E: Enforce strict mathematical consistency (Total = Quantity * Price)
        if triplet:
            q_col, p_col, t_col = triplet
            chunk[q_col] = chunk[q_col].round(0)
            inconsistent = chunk[t_col].isna() | (np.abs(chunk[t_col] - (chunk[q_col] * chunk[p_col])) > 0.01)
            col_imputed_masks[t_col] |= inconsistent.to_numpy()
            chunk.loc[inconsistent, t_col] = (chunk.loc[inconsistent, q_col] * chunk.loc[inconsistent, p_col]).round(2)

        # 5. Outlier detection (Tukey 1.5x IQR) & text normalization with cell-level tracking
        row_outliers = np.zeros(len(chunk), dtype=bool)
        for nc in schema["numeric_cols"]:
            if nc in chunk.columns and nc not in schema["id_cols"]:
                row_outliers |= (chunk[nc].to_numpy() > schema["stats"][nc]["upper_fence"])

        for tc in schema["cat_cols"]:
            if tc in chunk.columns and tc not in schema["date_cols"]:
                raw_str = chunk[tc].fillna("").astype(str).str.strip()
                col_imputed_masks[tc] = raw_str.isin(ALL_DIRTY_TOKENS).to_numpy()
                chunk[tc] = normalize_text_series(chunk[tc])

        chunk["is_outlier"] = row_outliers

        # Add column-level imputation flags & accurate row-level is_imputed
        row_any_imputed = np.zeros(len(chunk), dtype=bool)
        ordered_cols = schema["numeric_cols"] + schema["cat_cols"]
        for col_name in ordered_cols:
            if col_name in col_imputed_masks:
                mask = col_imputed_masks[col_name]
                imputed_cells += int(mask.sum())
                row_any_imputed |= mask
                if not schema["is_fleet"]:
                    chunk[f"{col_name}_imputed"] = mask

        chunk["is_imputed"] = row_any_imputed
        outliers_flagged += int(row_outliers.sum())
        clean_rows += len(chunk)

        if schema["is_fleet"]:
            out_cols = ["transaction_id", "vehicle_id", "timestamp", "distance_km", "amount_inr", "txn_type", "is_outlier"]
            chunk = chunk[out_cols]

        chunk.to_csv(output_path, mode="w" if first_chunk else "a", header=first_chunk, index=False)
        first_chunk = False

    elapsed = round(time.perf_counter() - start_time, 2)
    report = {
        "run_at": datetime.now().isoformat(),
        "schema_mode": "fleet_telemetry" if schema["is_fleet"] else "universal_auto_detected",
        "detected_columns": {
            "id_columns": schema["id_cols"],
            "date_columns": schema["date_cols"],
            "numeric_columns": schema["numeric_cols"],
            "lookup_relationships": [f"{c} -> {n}" for (c, n) in schema["lookup_maps"].keys()],
        },
        "performance": {
            "elapsed_seconds": elapsed,
            "throughput_rows_per_sec": int(initial_rows / elapsed) if elapsed > 0 else initial_rows,
        },
        "row_summary": {
            "initial_rows": initial_rows,
            "clean_rows": clean_rows,
            "total_dropped": initial_rows - clean_rows,
            "retention_rate_pct": round((clean_rows / initial_rows) * 100, 2) if initial_rows else 0,
        },
        "dropped_breakdown": drop_log,
        "transformations": {
            "cells_repaired_or_flagged": imputed_cells,
            "invalid_dates_preserved": missing_dates_flagged,
            "outliers_flagged_iqr": outliers_flagged,
        },
    }

    with open("data/quality_audit_report.json", "w") as f:
        json.dump(report, f, indent=4)

    print(json.dumps(report, indent=4))
    return report


if __name__ == "__main__":
    run_etl()