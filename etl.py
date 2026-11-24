import json
import os
import re
import sqlite3
import tempfile
import time
from collections import Counter, defaultdict
from datetime import datetime
import numpy as np
import pandas as pd

FX_RATES = {"INR": 1.0, "USD": 84.0, "EUR": 91.5}
CHUNK_SIZE = 100_000
RESERVOIR_SIZE = 50_000
NUM_PATTERN = re.compile(r"^[\s₹$€£+-]*[\d,]+(\.\d+)?[\s]*$")

MISSING_TOKENS = {"", "nan", "NaN", "None", "none", "null", "NULL", "N/A", "n/a", "NA", "na", "-"}
ERROR_TOKENS = {"ERROR", "error", "#REF!", "#VALUE!", "#DIV/0!", "#N/A", "INVALID", "invalid"}
UNKNOWN_TOKENS = {"UNKNOWN", "unknown"}
ALL_DIRTY_TOKENS = list(MISSING_TOKENS | ERROR_TOKENS | UNKNOWN_TOKENS)


class StreamingReservoir:
    """Fixed-memory reservoir sampler (Algorithm R) + Welford online stats for flat RAM usage."""

    def __init__(self, max_size=RESERVOIR_SIZE, seed=42):
        self.max_size = max_size
        self.rng = np.random.default_rng(seed)
        self.buffer = np.empty(max_size, dtype="float64")
        self.count = 0
        self.mean = 0.0
        self.m2 = 0.0
        self.min_val = np.inf
        self.max_val = -np.inf

    def update(self, arr):
        arr = np.asarray(arr, dtype="float64")
        arr = arr[np.isfinite(arr)]
        n = len(arr)
        if n == 0:
            return

        self.min_val = min(self.min_val, float(arr.min()))
        self.max_val = max(self.max_val, float(arr.max()))

        batch_mean = float(arr.mean())
        batch_m2 = float(((arr - batch_mean) ** 2).sum())
        if self.count == 0:
            self.mean = batch_mean
            self.m2 = batch_m2
        else:
            total = self.count + n
            delta = batch_mean - self.mean
            self.mean += delta * (n / total)
            self.m2 += batch_m2 + (delta ** 2) * (self.count * n / total)

        if self.count < self.max_size:
            take = min(self.max_size - self.count, n)
            self.buffer[self.count : self.count + take] = arr[:take]
            self.count += take
            arr = arr[take:]
            n = len(arr)

        if n > 0:
            indices = np.arange(self.count + 1, self.count + n + 1)
            rand_idx = self.rng.integers(0, indices)
            mask = rand_idx < self.max_size
            self.buffer[rand_idx[mask]] = arr[mask]
            self.count += n

    def summary(self):
        if self.count == 0:
            return None
        sample = self.buffer[: min(self.count, self.max_size)]
        q1, med, q3 = np.percentile(sample, [25, 50, 75])
        iqr = q3 - q1
        return {
            "median": float(med),
            "mean": round(self.mean, 2),
            "min": round(self.min_val, 2),
            "max": round(self.max_val, 2),
            "lower_fence": float(q1 - 1.5 * iqr),
            "upper_fence": float(q3 + 1.5 * iqr),
        }


class DiskDeduplicator:
    """SQLite-backed primary key index keeping deduplication memory flat across 1M+ rows."""

    def __init__(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode = OFF;")
        self.conn.execute("PRAGMA synchronous = OFF;")
        self.conn.execute("CREATE TABLE seen (pk TEXT PRIMARY KEY);")

    def filter_new(self, ids_series):
        ids = ids_series.astype(str).tolist()
        keep_mask = np.zeros(len(ids), dtype=bool)
        cur = self.conn.cursor()
        for i, val in enumerate(ids):
            cur.execute("INSERT OR IGNORE INTO seen (pk) VALUES (?)", (val,))
            if cur.rowcount == 1:
                keep_mask[i] = True
        self.conn.commit()
        return keep_mask

    def close(self):
        self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)


def clean_col_name(col):
    return re.sub(r"[^a-z0-9]+", "_", str(col).strip().lower()).strip("_")


def coerce_dirty_numeric(series):
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
    qty_col = next((c for c in numeric_cols if any(k in c for k in ["qty", "quantity", "units", "count"])), None)
    price_col = next((c for c in numeric_cols if any(k in c for k in ["price", "rate", "unit_cost", "cost_per"])), None)
    total_col = next((c for c in numeric_cols if any(k in c for k in ["total", "amount", "revenue", "sales", "spend"])), None)
    if qty_col and price_col and total_col and len({qty_col, price_col, total_col}) == 3:
        return (qty_col, price_col, total_col)
    return None


def profile_schema(input_path):
    """Pass 1: Multi-chunk streaming schema profiling with fixed-size reservoirs."""
    header_df = pd.read_csv(input_path, nrows=0)
    raw_cols = list(header_df.columns)
    norm_cols = [clean_col_name(c) for c in raw_cols]
    col_map = dict(zip(raw_cols, norm_cols))

    is_fleet = {"transaction_id", "vehicle_id", "amount", "currency"}.issubset(set(norm_cols))
    id_cols = [c for c in norm_cols if c == "id" or c.endswith("_id")]
    date_cols = [c for c in norm_cols if any(k in c for k in ["date", "time", "timestamp", "created"])]
    candidate_cols = [c for c in norm_cols if c not in id_cols and c not in date_cols]

    numeric_votes = Counter()
    total_chunks = 0
    reservoirs = {c: StreamingReservoir() for c in candidate_cols}
    cat_samples = []

    for chunk in pd.read_csv(input_path, chunksize=CHUNK_SIZE):
        total_chunks += 1
        chunk = chunk.rename(columns=col_map)
        for c in candidate_cols:
            converted = coerce_dirty_numeric(chunk[c])
            if pd.api.types.is_numeric_dtype(converted):
                numeric_votes[c] += 1
                s = converted.dropna()
                if not s.empty:
                    reservoirs[c].update(s.to_numpy(dtype="float64"))
        if len(cat_samples) < 10:
            cat_samples.append(chunk.head(500))

    numeric_cols = [c for c in candidate_cols if total_chunks > 0 and (numeric_votes[c] / total_chunks) >= 0.80]
    cat_cols = [c for c in candidate_cols if c not in numeric_cols]

    stats = {}
    for c in numeric_cols:
        s = reservoirs[c].summary()
        if s is not None:
            stats[c] = s

    triplet = detect_math_triplet(list(stats.keys()))
    lookup_maps = {}
    if cat_samples:
        strat_df = pd.concat(cat_samples, ignore_index=True)
        for cc in cat_cols:
            clean_cat = strat_df[cc].astype(str).str.strip()
            valid_cat = ~clean_cat.isin(ALL_DIRTY_TOKENS) & strat_df[cc].notna()
            if valid_cat.sum() < 2 or clean_cat[valid_cat].nunique() > 200:
                continue
            for nc in numeric_cols:
                if triplet and nc == triplet[2]:
                    continue  # Never treat row totals as a fixed lookup constant per item
                num_s = coerce_dirty_numeric(strat_df[nc])
                both_valid = valid_cat & num_s.notna()
                if both_valid.sum() < 2:
                    continue
                grp_std = num_s[both_valid].groupby(clean_cat[both_valid]).std().dropna()
                if grp_std.empty or (grp_std <= 0.01).mean() >= 0.90:
                    lookup_maps[(cc, nc)] = num_s[both_valid].groupby(clean_cat[both_valid]).median().to_dict()

    return {
        "col_map": col_map,
        "is_fleet": is_fleet,
        "id_cols": id_cols,
        "date_cols": date_cols,
        "numeric_cols": list(stats.keys()),
        "cat_cols": cat_cols,
        "lookup_maps": lookup_maps,
        "math_triplet": triplet,
        "stats": stats,
    }


def normalize_text_series(series):
    res = series.fillna("MISSING").astype(str).str.strip()
    res = res.replace({tok: "MISSING" for tok in MISSING_TOKENS})
    res = res.replace({tok: "INVALID_ENTRY" for tok in ERROR_TOKENS})
    res = res.replace({tok: "UNKNOWN" for tok in UNKNOWN_TOKENS})
    return res


def run_etl(
    input_path="data/raw_transactions.csv",
    output_path="data/clean_transactions.csv",
    report_path="data/quality_audit_report.json",
):
    start_time = time.perf_counter()
    schema = profile_schema(input_path)

    dedup = DiskDeduplicator()
    initial_rows = 0
    clean_rows = 0
    imputed_cells = 0
    missing_dates_flagged = 0
    outliers_flagged = 0
    math_conflicts_flagged = 0
    drop_log = {"duplicates": 0, "null_dates_or_empty": 0, "invalid_values": 0}
    first_chunk = True

    monthly_spend = defaultdict(float)
    category_spend = defaultdict(float)
    vehicle_agg = defaultdict(lambda: [0, 0.0, 0.0, 0])
    cat_counts = Counter()
    preview_rows = []

    pk_col = schema["id_cols"][0] if schema["id_cols"] else None

    try:
        for chunk in pd.read_csv(input_path, chunksize=CHUNK_SIZE):
            initial_rows += len(chunk)
            chunk = chunk.rename(columns=schema["col_map"])

            # 1. Drop blank rows & disk-backed deduplication
            before = len(chunk)
            chunk = chunk.dropna(how="all")
            if pk_col and pk_col in chunk.columns:
                chunk = chunk.dropna(subset=[pk_col]).drop_duplicates(subset=[pk_col], keep="first")
                is_new = dedup.filter_new(chunk[pk_col])
                chunk = chunk[is_new].copy()
            else:
                chunk = chunk.drop_duplicates().copy()
            drop_log["duplicates"] += before - len(chunk)

            # 2. Date parsing
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

            # 3. Fleet FX & negative charge filter
            if schema["is_fleet"]:
                before = len(chunk)
                chunk["currency"] = chunk["currency"].fillna("UNKNOWN").astype(str).str.strip().str.upper()
                chunk = chunk[chunk["currency"].isin(FX_RATES.keys())].copy()
                drop_log["invalid_values"] += before - len(chunk)
                chunk["amount_inr"] = np.round(
                    pd.to_numeric(chunk["amount"], errors="coerce") * chunk["currency"].map(FX_RATES), 2
                )

                bad_neg = (chunk["amount_inr"] <= 0) & (chunk["txn_type"].astype(str).str.upper() != "REFUND")
                drop_log["invalid_values"] += int(bad_neg.sum())
                chunk = chunk[~bad_neg].copy()

            col_imputed_masks = {}

            # 4. Universal numeric coercion & non-destructive reconciliation
            for nc in schema["numeric_cols"]:
                if nc not in chunk.columns:
                    continue
                chunk[nc] = coerce_dirty_numeric(chunk[nc])
                col_imputed_masks[nc] = chunk[nc].isna().to_numpy().copy()

            triplet = schema["math_triplet"]
            if triplet:
                q_col, p_col, t_col = triplet
                m_tot = chunk[t_col].isna() & chunk[q_col].notna() & chunk[p_col].notna()
                chunk.loc[m_tot, t_col] = chunk.loc[m_tot, q_col] * chunk.loc[m_tot, p_col]

                m_qty = chunk[q_col].isna() & chunk[t_col].notna() & (chunk[p_col] > 0)
                chunk.loc[m_qty, q_col] = (chunk.loc[m_qty, t_col] / chunk.loc[m_qty, p_col]).round(0)

                m_prc = chunk[p_col].isna() & chunk[t_col].notna() & (chunk[q_col] > 0)
                chunk.loc[m_prc, p_col] = (chunk.loc[m_prc, t_col] / chunk.loc[m_prc, q_col]).round(2)

            for (cc, nc), mapping in schema["lookup_maps"].items():
                if cc in chunk.columns and nc in chunk.columns:
                    miss_mask = chunk[nc].isna()
                    if miss_mask.any():
                        chunk.loc[miss_mask, nc] = chunk.loc[miss_mask, cc].astype(str).str.strip().map(mapping)

            if triplet:
                q_col, p_col, t_col = triplet
                m_tot = chunk[t_col].isna() & chunk[q_col].notna() & chunk[p_col].notna()
                chunk.loc[m_tot, t_col] = chunk.loc[m_tot, q_col] * chunk.loc[m_tot, p_col]

                m_qty = chunk[q_col].isna() & chunk[t_col].notna() & (chunk[p_col] > 0)
                chunk.loc[m_qty, q_col] = (chunk.loc[m_qty, t_col] / chunk.loc[m_qty, p_col]).round(0)

            for nc in schema["numeric_cols"]:
                if nc not in chunk.columns:
                    continue
                if triplet and nc == triplet[2]:
                    continue
                chunk[nc] = chunk[nc].fillna(schema["stats"][nc]["median"]).round(2)

            if triplet:
                q_col, p_col, t_col = triplet
                chunk[q_col] = chunk[q_col].round(0)
                calc_total = (chunk[q_col] * chunk[p_col]).round(2)
                still_missing_tot = chunk[t_col].isna()
                chunk.loc[still_missing_tot, t_col] = calc_total[still_missing_tot]
                conflict_mask = np.abs(chunk[t_col] - calc_total) > 0.01
                chunk["calculated_total"] = calc_total
                chunk["math_conflict"] = conflict_mask
                math_conflicts_flagged += int(conflict_mask.sum())

            # 5. Two-sided Tukey 1.5x IQR outlier flags (column-specific + row-level)
            row_outliers = np.zeros(len(chunk), dtype=bool)
            for nc in schema["numeric_cols"]:
                if nc in chunk.columns and nc not in schema["id_cols"]:
                    vals = chunk[nc].to_numpy()
                    col_out = (vals < schema["stats"][nc]["lower_fence"]) | (vals > schema["stats"][nc]["upper_fence"])
                    row_outliers |= col_out
                    if not schema["is_fleet"]:
                        chunk[f"{nc}_outlier"] = col_out

            for tc in schema["cat_cols"]:
                if tc in chunk.columns and tc not in schema["date_cols"]:
                    raw_str = chunk[tc].fillna("").astype(str).str.strip()
                    col_imputed_masks[tc] = raw_str.isin(ALL_DIRTY_TOKENS).to_numpy()
                    chunk[tc] = normalize_text_series(chunk[tc])

            chunk["is_outlier"] = row_outliers
            row_any_imputed = np.zeros(len(chunk), dtype=bool)
            for col_name in schema["numeric_cols"] + schema["cat_cols"]:
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

                months = pd.to_datetime(chunk["timestamp"]).dt.strftime("%Y-%m")
                for m, amt in chunk.groupby(months)["amount_inr"].sum().items():
                    monthly_spend[m] += float(amt)
                for cat, amt in chunk.groupby("txn_type")["amount_inr"].sum().items():
                    category_spend[cat] += float(amt)
                for vid, grp in chunk.groupby("vehicle_id"):
                    rec = vehicle_agg[int(vid)]
                    rec[0] += len(grp)
                    rec[1] += float(grp["distance_km"].sum())
                    rec[2] += float(grp["amount_inr"].sum())
                    rec[3] += int(grp["is_outlier"].sum())
            else:
                if schema["cat_cols"]:
                    cat_counts.update(chunk[schema["cat_cols"][0]].value_counts().to_dict())
                if len(preview_rows) < 10:
                    preview_rows.extend(chunk.head(10 - len(preview_rows)).to_dict(orient="records"))

            os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
            chunk.to_csv(output_path, mode="w" if first_chunk else "a", header=first_chunk, index=False)
            first_chunk = False
    finally:
        dedup.close()

    if schema["is_fleet"]:
        total_spend = sum(v[2] for v in vehicle_agg.values())
        total_dist = sum(v[1] for v in vehicle_agg.values())
        top_vehicles = [
            {
                "vehicle_id": vid,
                "trips": trips,
                "dist": round(dist, 1),
                "spend": round(spend, 2),
                "cost_per_km": round(spend / dist, 2) if dist > 0 else 0.0,
                "outliers": out_cnt,
            }
            for vid, (trips, dist, spend, out_cnt) in vehicle_agg.items()
        ]
        top_vehicles.sort(key=lambda x: x["cost_per_km"], reverse=True)
        sorted_months = sorted(monthly_spend.keys())
        sorted_cats = sorted(category_spend.items(), key=lambda x: x[1], reverse=True)
        dashboard_cache = {
            "mode": "fleet",
            "kpis": {
                "c1_label": "Total Fleet Spend",
                "c1_val": f"₹{total_spend:,.2f}",
                "c2_label": "Total Distance",
                "c2_val": f"{total_dist:,.1f} km",
                "c3_label": "Avg Cost / km",
                "c3_val": f"₹{(total_spend / total_dist if total_dist else 0):.2f}/km",
                "c4_label": "Outliers Flagged",
                "c4_val": f"{outliers_flagged:,}",
                "c5_label": "Clean Records",
                "c5_val": f"{clean_rows:,}",
            },
            "chart1": {
                "title": "Monthly Expenditure (INR)",
                "labels": sorted_months,
                "values": [round(monthly_spend[m], 2) for m in sorted_months],
            },
            "chart2": {
                "title": "Spend by Category",
                "labels": [c[0] for c in sorted_cats],
                "values": [round(c[1], 2) for c in sorted_cats],
            },
            "table_rows": top_vehicles[:10],
            "preview": [],
        }
    else:
        top_cats = cat_counts.most_common(10)
        num_summary = [
            {"column": c, "median": s["median"], "mean": s["mean"], "min": s["min"], "max": s["max"]}
            for c, s in schema["stats"].items()
        ]
        dashboard_cache = {
            "mode": "generic",
            "kpis": {
                "c1_label": "Clean Rows",
                "c1_val": f"{clean_rows:,}",
                "c2_label": "Numeric Columns",
                "c2_val": str(len(schema["numeric_cols"])),
                "c3_label": "Categorical Columns",
                "c3_val": str(len(schema["cat_cols"])),
                "c4_label": "Outliers Flagged",
                "c4_val": f"{outliers_flagged:,}",
                "c5_label": "Math Conflicts",
                "c5_val": f"{math_conflicts_flagged:,}",
            },
            "chart1": {
                "title": f"Top Categories ({schema['cat_cols'][0]})" if schema["cat_cols"] else "Categories",
                "labels": [c[0] for c in top_cats],
                "values": [c[1] for c in top_cats],
            },
            "chart2": {"title": "", "labels": [], "values": []},
            "table_rows": num_summary,
            "preview": preview_rows,
        }

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
            "math_conflicts_flagged": math_conflicts_flagged,
            "outliers_flagged_iqr": outliers_flagged,
        },
        "dashboard": dashboard_cache,
    }

    if report_path:
        os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)

    print(json.dumps({k: v for k, v in report.items() if k != "dashboard"}, indent=4))
    return report


if __name__ == "__main__":
    run_etl()