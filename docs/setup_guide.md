# Setup Guide

Complete step-by-step instructions to get the pipeline running from scratch.

---

## Prerequisites

| Tool | Version | Required | Install |
|---|---|---|---|
| Python | 3.9+ | ✅ Yes | [python.org](https://www.python.org/downloads/) |
| pip | Latest | ✅ Yes | Included with Python |
| Power BI Desktop | Latest | ✅ For dashboard | [Microsoft](https://powerbi.microsoft.com/desktop/) |
| SQLite ODBC Driver | 3.x | For Power BI→SQLite | [ch-werner.de](http://www.ch-werner.de/sqliteodbc/) |
| Git | Any | For cloning | [git-scm.com](https://git-scm.com/) |

---

## Step 1 — Clone the Repository

```bash
git clone https://github.com/yourusername/bi-reporting-system.git
cd bi-reporting-system
```

---

## Step 2 — Create a Virtual Environment

```bash
# Windows
python -m venv .venv
.venv\Scripts\activate

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
```

---

## Step 3 — Install Dependencies

```bash
pip install -r requirements.txt
```

Verify key packages installed:
```bash
python -c "import pandas, numpy, scipy, openpyxl, yaml; print('✅ All packages OK')"
```

---

## Step 4 — Generate the Dataset

The synthetic dataset is not included in the repository (it's generated locally):

```bash
python generate_dataset.py
```

Expected output:
```
Generating 10,800 rows of sales data...
✅ Saved 10,800 rows → data/raw/sales_operations.csv

Column summary:
transaction_id     object
date               object
...
Revenue range: $5.00 – $72,600.00
Date range:    2022-01-01 → 2024-12-31
```

---

## Step 5 — Run the Full Pipeline

```bash
python run_pipeline.py
```

Expected output:
```
╔══════════════════════════════════════════════════════════╗
║   Automated Business Intelligence Reporting System       ║
╚══════════════════════════════════════════════════════════╝

2024-01-15 10:30:00 | INFO     | Pipeline started  | 2024-01-15 10:30:00
2024-01-15 10:30:00 | INFO     | Stage:            | all
...
STAGE 1: Data Ingestion
STAGE 2: SQL Transformations
STAGE 3a: Anomaly Detection
STAGE 3b: Report Generation

PIPELINE EXECUTION SUMMARY
══════════════════════════
  ✅ SUCCESS  ingest                         1.23s
  ✅ SUCCESS  transform                      2.45s
  ✅ SUCCESS  anomaly                        0.87s
  ✅ SUCCESS  report                         3.12s

  Total elapsed: 7.67s

🎉 All stages completed successfully!
   → Excel report: data/processed/report_summary_20240115_103007.xlsx
   → Anomalies:    data/processed/anomalies.csv
   → Database:     data/pipeline.db
   → Logs:         logs/pipeline_2024-01-15.log
```

---

## Step 6 — Open the Excel Report

Navigate to `data/processed/` and open `report_summary_*.xlsx`.

Tabs included:
- **Executive Summary** — yearly KPIs
- **Monthly Trends** — MoM and YoY growth
- **Regional Performance** — by region and year
- **Product Analysis** — category and product breakdown
- **Anomaly Report** — flagged transactions

---

## Step 7 — Open the Power BI Dashboard

1. Open Power BI Desktop
2. **File → Open → Browse** to `dashboards/powerbi/BI_Reporting_System.pbix`
3. Connect to the SQLite database:
   - **Home → Transform Data → Data Source Settings**
   - Update the path to your local `data/pipeline.db`
4. Click **Refresh** to load the latest data

> See `dashboards/powerbi/README_POWERBI.md` for full connection instructions.

---

## Step 8 — Run Tests

```bash
pytest tests/ -v
```

Expected:
```
tests/test_ingestion.py::TestRequiredColumns::test_all_required_columns_defined PASSED
tests/test_ingestion.py::TestValidation::test_valid_df_passes_validation PASSED
...
tests/test_transformations.py::TestCoreLayerTransformation::test_gross_profit_calculated_correctly PASSED
...
tests/test_anomaly_detection.py::TestZScoreDetection::test_obvious_outliers_detected PASSED
...
========================= 35 passed in 2.14s =========================
```

---

## Running Individual Pipeline Stages

```bash
# Ingestion only (reload CSV into staging)
python run_pipeline.py --stage ingest

# SQL transformations only
python run_pipeline.py --stage transform

# Anomaly detection only
python run_pipeline.py --stage anomaly

# Report generation only
python run_pipeline.py --stage report

# Generate fresh dataset then run all
python run_pipeline.py --generate-data
```

---

## Switching to SQL Server

1. Install pyodbc: `pip install pyodbc`
2. Install ODBC Driver 18 for SQL Server
3. Update `config/config.yaml`:

```yaml
database:
  engine:   sqlserver
  server:   YOUR_SERVER_NAME
  database: BIReportingDB
  username: sa
  password: YOUR_PASSWORD
```

4. Run the pipeline — DDL and DML will execute against SQL Server
5. For the stored procedure approach, run `sql/stored_procedures/sp_refresh_reporting.sql` in SSMS

---

## Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `FileNotFoundError: data/raw/sales_operations.csv` | Dataset not generated | Run `python generate_dataset.py` |
| `ModuleNotFoundError: No module named 'openpyxl'` | Dependencies not installed | Run `pip install -r requirements.txt` |
| `sqlite3.OperationalError: no such table` | Schema not created | Run `python run_pipeline.py --stage transform` |
| `PermissionError` on Excel file | File open in Excel | Close the Excel file and re-run |
| Power BI "Cannot connect" | Wrong DB path | Update data source path in Power BI |
| Tests failing | Missing dataset | Run `python generate_dataset.py` first |
