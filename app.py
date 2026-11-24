import json
import os
import numpy as np
import pandas as pd
from flask import Flask, redirect, render_template, request, send_file, url_for
from etl import run_etl
from tests.generate_data import generate_data  # TEMP: remove before Git push

app = Flask(__name__)

RAW_CSV = "data/raw_transactions.csv"
CLEAN_CSV = "data/clean_transactions.csv"
AUDIT_JSON = "data/quality_audit_report.json"


def build_analytics():
    audit = {}
    if os.path.exists(AUDIT_JSON):
        with open(AUDIT_JSON, "r") as f:
            audit = json.load(f)

    if not os.path.exists(CLEAN_CSV):
        return "empty", audit, {}, {"labels": [], "values": []}, {"labels": [], "values": []}, [], []

    df = pd.read_csv(CLEAN_CSV)
    if df.empty:
        return "empty", audit, {}, {"labels": [], "values": []}, {"labels": [], "values": []}, [], []

    # Mode 1: Fleet Telemetry Schema
    if {"amount_inr", "distance_km", "vehicle_id", "txn_type"}.issubset(df.columns):
        total_spend = float(df["amount_inr"].sum())
        total_dist = float(df["distance_km"].sum())
        kpis = {
            "c1_label": "Clean Records", "c1_val": f"{len(df):,}",
            "c2_label": "Total Spend", "c2_val": f"₹{total_spend:,.2f}",
            "c3_label": "Total Distance", "c3_val": f"{total_dist:,.1f} km",
            "c4_label": "Avg Cost / km", "c4_val": f"₹{(total_spend / total_dist):.2f}/km" if total_dist > 0 else "₹0/km",
            "c5_label": "IQR Outliers", "c5_val": f"{int(df['is_outlier'].sum()):,}",
        }
        df["month"] = df["timestamp"].astype(str).str.slice(0, 7)
        monthly = df.groupby("month")["amount_inr"].sum().round(2).sort_index()
        by_type = df.groupby("txn_type")["amount_inr"].sum().round(2).sort_values(ascending=False)

        veh = df.groupby("vehicle_id").agg(
            trips=("transaction_id", "count"),
            dist=("distance_km", "sum"),
            spend=("amount_inr", "sum"),
            outliers=("is_outlier", "sum"),
        ).reset_index()
        veh["cost_per_km"] = (veh["spend"] / veh["dist"]).round(2)
        top_vehicles = veh.sort_values("cost_per_km", ascending=False).head(10).round(2).to_dict(orient="records")

        return (
            "fleet",
            audit,
            kpis,
            {"labels": monthly.index.tolist(), "values": monthly.values.tolist(), "title": "Monthly Fleet Spend (INR)"},
            {"labels": by_type.index.tolist(), "values": by_type.values.tolist(), "title": "Spend by Category"},
            top_vehicles,
            [],
        )

    # Mode 2: Any Other Uploaded Table (Universal Profiling)
    num_cols = [c for c in df.select_dtypes(include=[np.number]).columns if not c.endswith("_id") and c != "id"]
    cat_cols = [c for c in df.select_dtypes(include="object").columns if df[c].nunique() <= 30]
    outliers = int(df["is_outlier"].sum()) if "is_outlier" in df.columns else 0

    kpis = {
        "c1_label": "Clean Rows", "c1_val": f"{len(df):,}",
        "c2_label": "Total Columns", "c2_val": str(len(df.columns)),
        "c3_label": "Numeric Metrics", "c3_val": str(len(num_cols)),
        "c4_label": "Retention Rate", "c4_val": f"{audit.get('row_summary', {}).get('retention_rate_pct', 100)}%",
        "c5_label": "IQR Outliers", "c5_val": f"{outliers:,}",
    }

    bar_chart = {"labels": [], "values": [], "title": "Top Categories"}
    if cat_cols and num_cols:
        grp = df.groupby(cat_cols[0])[num_cols[0]].sum().round(2).sort_values(ascending=False).head(10)
        bar_chart = {"labels": grp.index.tolist(), "values": grp.values.tolist(), "title": f"Total {num_cols[0]} by {cat_cols[0]}"}
    elif cat_cols:
        cnt = df[cat_cols[0]].value_counts().head(10)
        bar_chart = {"labels": cnt.index.tolist(), "values": cnt.values.tolist(), "title": f"Row Count by {cat_cols[0]}"}

    num_summary = []
    for c in num_cols:
        num_summary.append({
            "column": c,
            "median": round(float(df[c].median()), 2),
            "mean": round(float(df[c].mean()), 2),
            "min": round(float(df[c].min()), 2),
            "max": round(float(df[c].max()), 2),
        })

    preview_rows = df.head(10).to_dict(orient="records")
    return "generic", audit, kpis, bar_chart, {"labels": [], "values": [], "title": ""}, num_summary, preview_rows


@app.route("/")
def results():
    msg = request.args.get("msg", "")
    mode, audit, kpis, chart1, chart2, table_rows, preview = build_analytics()
    return render_template(
        "dashboard.html",
        mode=mode,
        msg=msg,
        audit=audit,
        kpis=kpis,
        chart1=chart1,
        chart2=chart2,
        table_rows=table_rows,
        preview=preview,
        has_clean=os.path.exists(CLEAN_CSV),
    )


@app.route("/generate", methods=["POST"])
def generate():
    rows = int(request.form.get("rows", 100000))
    generate_data(n_rows=rows, seed=None)
    return redirect(url_for("results", msg=f"Generated {rows:,} random messy rows. Click '2. Clean Data' to process."))


@app.route("/clean", methods=["POST"])
def clean():
    if not os.path.exists(RAW_CSV):
        return redirect(url_for("results", msg="No raw data found. Generate or upload a CSV first."))
    report = run_etl(RAW_CSV, CLEAN_CSV)
    return redirect(url_for("results", msg=f"Cleaned {report['row_summary']['clean_rows']:,} rows ({report['schema_mode']})."))


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("file")
    if not file or not file.filename.endswith(".csv"):
        return redirect(url_for("results", msg="Please select a valid .csv file."))
    file.save(RAW_CSV)
    report = run_etl(RAW_CSV, CLEAN_CSV)
    return redirect(url_for("results", msg=f"Uploaded & cleaned {report['row_summary']['clean_rows']:,} rows ({report['schema_mode']})."))


@app.route("/download")
def download():
    if os.path.exists(CLEAN_CSV):
        return send_file(CLEAN_CSV, as_attachment=True, download_name="cleaned_output.csv")
    return redirect(url_for("results", msg="No cleaned file available yet."))


if __name__ == "__main__":
    app.run(debug=True)