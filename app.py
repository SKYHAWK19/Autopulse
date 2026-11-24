import json
import os
import tempfile
import uuid
from flask import Flask, render_template, request, send_file
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename
from data_generator import generate_data
from etl import run_etl

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 50 * 1024 * 1024  # 50 MB max upload size

RAW_PATH = "data/raw_transactions.csv"
CLEAN_PATH = "data/clean_transactions.csv"
REPORT_PATH = "data/quality_audit_report.json"


def load_dashboard_state():
    audit = {}
    if os.path.exists(REPORT_PATH):
        with open(REPORT_PATH, "r", encoding="utf-8") as f:
            audit = json.load(f)
    dash = audit.get("dashboard", {})
    return (
        audit,
        dash.get("mode", "empty"),
        dash.get("kpis", {}),
        dash.get("chart1", {"title": "", "labels": [], "values": []}),
        dash.get("chart2", {"title": "", "labels": [], "values": []}),
        dash.get("table_rows", []),
        dash.get("preview", []),
    )


@app.errorhandler(RequestEntityTooLarge)
def handle_large_file(_err):
    audit, mode, kpis, c1, c2, table_rows, preview = load_dashboard_state()
    return (
        render_template(
            "dashboard.html",
            audit=audit,
            mode=mode,
            kpis=kpis,
            chart1=c1,
            chart2=c2,
            table_rows=table_rows,
            preview=preview,
            has_clean=os.path.exists(CLEAN_PATH),
            msg="Upload rejected: File exceeds the 50 MB size limit.",
        ),
        413,
    )


@app.route("/", methods=["GET"])
def index():
    audit, mode, kpis, c1, c2, table_rows, preview = load_dashboard_state()
    return render_template(
        "dashboard.html",
        audit=audit,
        mode=mode,
        kpis=kpis,
        chart1=c1,
        chart2=c2,
        table_rows=table_rows,
        preview=preview,
        has_clean=os.path.exists(CLEAN_PATH),
        msg=request.args.get("msg", ""),
    )


@app.route("/generate", methods=["POST"])
def generate():
    rows = min(max(int(request.form.get("rows", 100000)), 1000), 1_000_000)
    generate_data(rows, RAW_PATH)
    run_etl(RAW_PATH, CLEAN_PATH, REPORT_PATH)
    audit, mode, kpis, c1, c2, table_rows, preview = load_dashboard_state()
    return render_template(
        "dashboard.html",
        audit=audit,
        mode=mode,
        kpis=kpis,
        chart1=c1,
        chart2=c2,
        table_rows=table_rows,
        preview=preview,
        has_clean=True,
        msg=f"Generated {rows:,} messy fleet records and executed streaming ETL.",
    )


@app.route("/clean", methods=["POST"])
def clean():
    if not os.path.exists(RAW_PATH):
        generate_data(100000, RAW_PATH)
    run_etl(RAW_PATH, CLEAN_PATH, REPORT_PATH)
    audit, mode, kpis, c1, c2, table_rows, preview = load_dashboard_state()
    return render_template(
        "dashboard.html",
        audit=audit,
        mode=mode,
        kpis=kpis,
        chart1=c1,
        chart2=c2,
        table_rows=table_rows,
        preview=preview,
        has_clean=True,
        msg="Cleaned existing dataset and refreshed quality audit report.",
    )


@app.route("/upload", methods=["POST"])
def upload():
    file = request.files.get("file")
    safe_name = secure_filename(file.filename) if file and file.filename else ""
    if not safe_name.lower().endswith(".csv"):
        audit, mode, kpis, c1, c2, table_rows, preview = load_dashboard_state()
        return render_template(
            "dashboard.html",
            audit=audit,
            mode=mode,
            kpis=kpis,
            chart1=c1,
            chart2=c2,
            table_rows=table_rows,
            preview=preview,
            has_clean=os.path.exists(CLEAN_PATH),
            msg="Please upload a valid .csv file.",
        )

    temp_upload = os.path.join(tempfile.gettempdir(), f"autopulse_{uuid.uuid4().hex}_{safe_name}")
    try:
        file.save(temp_upload)
        run_etl(temp_upload, CLEAN_PATH, REPORT_PATH)
        msg = f"Auto-detected schema and cleaned '{safe_name}'."
    except Exception as exc:
        msg = f"Failed to process '{safe_name}': {exc}"
    finally:
        if os.path.exists(temp_upload):
            os.remove(temp_upload)

    audit, mode, kpis, c1, c2, table_rows, preview = load_dashboard_state()
    return render_template(
        "dashboard.html",
        audit=audit,
        mode=mode,
        kpis=kpis,
        chart1=c1,
        chart2=c2,
        table_rows=table_rows,
        preview=preview,
        has_clean=os.path.exists(CLEAN_PATH),
        msg=msg,
    )


@app.route("/download", methods=["GET"])
def download():
    if os.path.exists(CLEAN_PATH):
        return send_file(CLEAN_PATH, as_attachment=True, download_name="cleaned_output.csv")
    return "No cleaned file available yet.", 404


if __name__ == "__main__":
    app.run(debug=os.getenv("FLASK_DEBUG", "false").lower() == "true")