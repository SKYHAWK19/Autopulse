# AutoPulse — Streaming ETL Pipeline & Fleet Cost Analytics

An end-to-end Data Analytics and Data Engineering project that ingests high-volume raw telemetry and arbitrary messy CSV datasets, enforces statistical and mathematical data integrity through a chunked Python ETL engine, loads normalized records into MySQL, and surfaces cost-efficiency insights via analytical SQL and interactive dashboards.

---

## Architecture & Pipeline Flow

```text
Raw CSV (1M+ Rows / Arbitrary Messy Tables)
        │
        ▼
[ etl.py ] ──► Pass 1: Schema Profiling, Lookup Learning & Global IQR Fences
           ──► Pass 2: Chunked Streaming Cleaning (100k rows/batch)
        │
        ├──► data/clean_transactions.csv
        ├──► data/quality_audit_report.json
        │
        ▼
[ load_db.py ] ──► Batch Load into MySQL (autopulse_db: vehicles & trip_logs)
        │
        ▼
[ sql/analysis.sql ] ──► CTEs, Window Functions (LAG, RANK) & vw_fleet_bi_summary
        │
        ▼
[ app.py / Looker Studio ] ──► Executive KPIs, Spend Trends & Vehicle ₹/km Rankings
```

---

## Core Capabilities

### 1. Dual-Mode Streaming ETL Engine (`etl.py`)
Built with **Pandas** and **NumPy** using `100,000`-row streaming chunks to maintain flat memory usage on `1,000,000+` row files:
* **Fleet Telemetry Mode:** Deduplicates `transaction_id`, parses timestamps, standardizes multi-currency transactions (`USD`, `EUR`, `INR`) into `INR`, filters invalid negative charges, imputes missing values via global medians, and flags Tukey $1.5 \times \text{IQR}$ anomalies (`is_outlier`).
* **Universal Schema-Agnostic Mode:** Automatically profiles any uploaded CSV:
  * **Header & Type Normalization:** Converts headers to `snake_case` and strips currency symbols (`₹`, `$`, `€`, `£`) and commas from dirty numeric strings.
  * **Deterministic Lookup & Algebraic Reconciliation:** Learns 1:1 categorical-to-numeric mappings (e.g., `item -> price_per_unit`) and runs a two-pass solver (`quantity × price = total`) to eliminate row-level arithmetic inconsistencies.
  * **Lossless Audit Trails:** Preserves records with missing/invalid dates via `is_date_missing`, separates text states into `MISSING`, `INVALID_ENTRY`, and `UNKNOWN`, and generates column-level `*_imputed` boolean flags alongside `quality_audit_report.json`.

### 2. Relational Warehouse & Analytical SQL (`sql/`)
* **`sql/schema.sql`:** Normalized MySQL schema (`autopulse_db`) with `vehicles` master dimension and `trip_logs` fact table indexed for time-series and vehicle-level aggregations.
* **`load_db.py`:** Foreign-key-safe batch loader (`10,000` rows per commit) using `mysql-connector-python`.
* **`sql/analysis.sql`:**
  * **Executive KPIs:** Fleet-wide spend (`INR`), distance (`km`), average cost per km (`₹/km`), and anomaly rate.
  * **Month-over-Month (MoM) Spend Growth:** Uses CTEs and `LAG()` / `SUM() OVER()` window functions to track monthly burn rate and cumulative spend.
  * **Vehicle Inefficiency Ranking:** Joins telemetry with vehicle specs and applies `RANK() OVER (ORDER BY cost_per_km DESC)` to isolate high-cost units.
  * **BI Analytical View (`vw_fleet_bi_summary`):** Pre-joined reporting layer ready for Looker Studio or Power BI.

### 3. Read-Only Analytical Portal (`app.py`)
A lightweight **Flask + Chart.js** results viewer that displays ETL audit benchmarks, monthly spend trends, category distributions, top cost-per-km vehicles, and an ad-hoc CSV cleaner with one-click download.

---

## Validated Quality & Performance Benchmarks

| Benchmark Metric | Fleet Telemetry Stress Test | Universal Messy Dataset (`dirty_cafe_sales.csv`) |
| :--- | :--- | :--- |
| **Input Volume** | 1,000,000+ rows (chunked) | 10,000 rows × 8 raw text columns |
| **Row Retention** | ~92.5% (drops duplicates & invalid FX/negatives) | **100.0%** (460 missing-date rows preserved & flagged) |
| **Missing (`NaN`) & `ERROR` Tokens** | 0 remaining | **0 remaining** (separated into `MISSING` / `INVALID_ENTRY`) |
| **Row Math Consistency** | FX-normalized to `INR` | **0 mismatches** (`quantity × price = total` across 10,000 rows) |
| **Outlier Detection ($1.5 \times \text{IQR}$)** | Flagged via `is_outlier` | **268 extreme orders** accurately isolated |

---

## Repository Structure

```text
autopulse/
├── app.py                     # Read-only Flask & Chart.js analytical results portal
├── etl.py                     # Dual-mode streaming ETL & schema-agnostic cleaner
├── load_db.py                 # Batch MySQL warehouse loader
├── bigquery_sync.py           # Google BigQuery export utility for cloud BI
├── sql/
│   ├── schema.sql             # MySQL database, tables, indexes & constraints
│   └── analysis.sql           # Analytical queries (CTEs, Window Functions) & BI view
├── templates/
│   └── dashboard.html         # Results dashboard & ad-hoc CSV cleaner UI
├── tests/
│   └── generate_data.py       # Synthetic messy telemetry generator for benchmarking
├── .env                       # Local database credentials (ignored by Git)
├── .gitignore
└── README.md
```

---

## Setup & Execution

1. **Create a virtual environment & install dependencies**
   ```bash
   python -m venv venv
   # Windows:
   .\venv\Scripts\activate
   # Linux/macOS:
   source venv/bin/activate

   pip install pandas numpy flask mysql-connector-python python-dotenv
   ```

2. **Configure environment variables (`.env`)**
   ```env
   DB_HOST=localhost
   DB_USER=root
   DB_PASSWORD=your_mysql_password
   DB_NAME=autopulse_db
   ```

3. **Initialize the MySQL schema**
   ```bash
   mysql -u root -p < sql/schema.sql
   ```

4. **Generate telemetry, run ETL, and load MySQL**
   ```bash
   python -c "from tests.generate_data import generate_data; generate_data(100000)"
   python etl.py
   python load_db.py
   ```

5. **Run analytical SQL queries & launch the results dashboard**
   ```bash
   # Execute analytical views & queries in MySQL:
   mysql -u root -p autopulse_db < sql/analysis.sql

   # Launch the local analytical report:
   python app.py
   ```
   Open `http://127.0.0.1:5000/` in your browser to view the results or upload any messy `.csv` file for automated cleaning.