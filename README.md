# 📊 Automated Business Intelligence Reporting System

> **Enterprise Data Reporting Automation** — End-to-end data pipeline simulation demonstrating automated analytics, SQL transformation, and Power BI dashboards for sales operations.

---

## 🎯 Project Overview

This project demonstrates a production-grade **automated BI reporting pipeline** that transforms raw sales data into actionable executive dashboards. It simulates the kind of analytics infrastructure used at enterprise scale — from data ingestion through SQL transformation to interactive Power BI reporting.

**Core pipeline:**
```
Raw CSV Data → Python Ingestion → SQLite/SQL Server → SQL Transformations → Automated Reports → Power BI Dashboard
```

---

## ✨ Features

| Feature | Description |
|---|---|
| 📥 **Data Ingestion** | Automated loading of raw sales CSV data with validation and logging |
| 🔄 **SQL Transformation** | Layered SQL views (staging → core → reporting) following medallion architecture |
| 📄 **Report Generation** | Automated PDF/Excel reports with scheduling support |
| 📊 **Power BI Dashboard** | 4-page interactive dashboard with executive KPIs, regional maps, product mix, and anomaly flags |
| 🚨 **Anomaly Detection** | Z-score and IQR-based statistical anomaly detection on revenue and cost |
| 🔁 **Pipeline Orchestration** | End-to-end runner with logging, error handling, and config-driven execution |

---

## 🗂️ Project Structure

```
bi-reporting-system/
│
├── data/
│   ├── raw/                        # Source CSV files
│   │   └── sales_operations.csv    # 3-year sales dataset (10,000+ rows)
│   └── processed/                  # Transformed outputs
│       ├── report_summary.xlsx     # Auto-generated Excel report
│       └── anomalies.csv           # Flagged anomalies
│
├── sql/
│   ├── transformations/
│   │   ├── 01_create_schema.sql    # Database and table setup
│   │   ├── 02_staging_layer.sql    # Raw → Staging transformations
│   │   ├── 03_core_layer.sql       # Staging → Core business logic
│   │   └── 04_reporting_layer.sql  # Core → Reporting aggregates
│   ├── views/
│   │   ├── vw_revenue_trends.sql   # Monthly revenue view
│   │   ├── vw_regional_performance.sql
│   │   ├── vw_product_analysis.sql
│   │   └── vw_anomaly_detection.sql
│   └── stored_procedures/
│       └── sp_refresh_reporting.sql
│
├── python/
│   ├── ingestion/
│   │   └── data_loader.py          # CSV ingestion with validation
│   ├── transformation/
│   │   └── sql_runner.py           # Executes SQL transformation layers
│   ├── reporting/
│   │   ├── report_generator.py     # Excel/PDF report automation
│   │   └── anomaly_detector.py     # Statistical anomaly detection
│   └── utils/
│       ├── db_connector.py         # Database connection manager
│       ├── logger.py               # Centralized logging
│       └── config_loader.py        # Config management
│
├── dashboards/
│   └── powerbi/
│       ├── BI_Reporting_System.pbix   # Power BI Desktop file
│       └── README_POWERBI.md          # Dashboard documentation
│
├── docs/
│   ├── architecture.md             # System architecture overview
│   ├── data_dictionary.md          # Column definitions and lineage
│   └── setup_guide.md             # Step-by-step setup instructions
│
├── tests/
│   ├── test_ingestion.py
│   ├── test_transformations.py
│   └── test_anomaly_detection.py
│
├── config/
│   └── config.yaml                 # Pipeline configuration
│
├── run_pipeline.py                 # 🚀 Main orchestrator
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- SQLite (built-in) or SQL Server
- Power BI Desktop (free from Microsoft)

### 1. Clone & Install

```bash
git clone https://github.com/yourusername/bi-reporting-system.git
cd bi-reporting-system
pip install -r requirements.txt
```

### 2. Run the Full Pipeline

```bash
python run_pipeline.py
```

This executes all four stages sequentially:
1. ✅ Ingest raw CSV into SQLite
2. ✅ Apply SQL transformation layers
3. ✅ Run anomaly detection
4. ✅ Generate Excel report to `data/processed/`

### 3. Open Power BI Dashboard

Open `dashboards/powerbi/BI_Reporting_System.pbix` in Power BI Desktop and refresh the data connection pointing to your local SQLite database.

---

## 📦 Dataset

**File:** `data/raw/sales_operations.csv`  
**Rows:** ~10,800 (3 years of daily sales data)  
**Source:** Synthetically generated to represent realistic enterprise sales operations

| Column | Type | Description |
|---|---|---|
| `transaction_id` | STRING | Unique transaction identifier |
| `date` | DATE | Transaction date (2022–2024) |
| `product_name` | STRING | Product sold |
| `product_category` | STRING | Electronics / Software / Services / Hardware |
| `region` | STRING | North / South / East / West / International |
| `sales_rep` | STRING | Sales representative name |
| `customer_segment` | STRING | Enterprise / SMB / Consumer |
| `revenue` | FLOAT | Transaction revenue (USD) |
| `cost` | FLOAT | Cost of goods sold (USD) |
| `units_sold` | INT | Number of units |
| `discount_pct` | FLOAT | Discount applied (0–40%) |
| `channel` | STRING | Direct / Partner / Online |
| `customer_id` | STRING | Customer identifier |

**Derived columns (via SQL):**
- `gross_profit` = revenue − cost
- `profit_margin` = gross_profit / revenue
- `yoy_growth` = year-over-year revenue growth %

---

## 🗄️ SQL Architecture

The pipeline follows a **3-layer medallion architecture**:

```
[Raw CSV]
    ↓
[Staging Layer]   — Type casting, null handling, deduplication
    ↓
[Core Layer]      — Business logic, joins, derived metrics
    ↓
[Reporting Layer] — Pre-aggregated views for dashboard consumption
```

Key views:
- `vw_revenue_trends` — Monthly revenue with MoM and YoY comparisons
- `vw_regional_performance` — Revenue, profit, and growth by region
- `vw_product_analysis` — Product mix, margin analysis, top performers
- `vw_anomaly_detection` — Z-score flagged statistical outliers

---

## 📊 Power BI Dashboard Pages

### Page 1 — Executive Summary
- Total Revenue (KPI card)
- Total Profit (KPI card)
- YoY Revenue Growth % (KPI card)
- Revenue Trend Line (24-month)
- Profit Margin Trend
- Revenue by Customer Segment (donut)

### Page 2 — Regional Performance
- Revenue by Region (bar chart)
- Profit Margin by Region (column chart)
- Regional Growth Heatmap (matrix)
- Map visual — Revenue by geography

### Page 3 — Product & Channel Analysis
- Revenue by Product Category (treemap)
- Top 10 Products by Revenue (bar)
- Channel Mix (stacked bar)
- Discount Impact on Profit (scatter)

### Page 4 — Anomaly Detection & Ops Metrics
- Anomaly flags table (Z-score > 2.5)
- Revenue vs Expected band chart
- Cost spikes timeline
- Sales rep performance outliers

---

## 🐍 Python Modules

### `run_pipeline.py`
Main orchestrator. Run stages independently:
```bash
python run_pipeline.py --stage ingest
python run_pipeline.py --stage transform
python run_pipeline.py --stage report
python run_pipeline.py --stage all   # default
```

### Anomaly Detection
Uses two methods:
- **Z-score** (threshold: ±2.5σ) for revenue and cost columns
- **IQR method** for discount percentage outliers

Results saved to `data/processed/anomalies.csv` and loaded into the Power BI anomaly page.

---

## ⚙️ Configuration

Edit `config/config.yaml` to customize:

```yaml
database:
  engine: sqlite          # sqlite | sqlserver | postgres
  path: data/pipeline.db

pipeline:
  ingest: true
  transform: true
  report: true

reporting:
  output_format: xlsx     # xlsx | pdf
  output_path: data/processed/

anomaly:
  zscore_threshold: 2.5
  iqr_multiplier: 1.5
```

---

## 🧪 Tests

```bash
pytest tests/ -v
```

---

## 🛠️ Tech Stack

| Tool | Role |
|---|---|
| **Python 3.9+** | Pipeline orchestration, data processing |
| **SQLite** | Local database engine (swap for SQL Server in prod) |
| **pandas** | Data manipulation and report generation |
| **openpyxl** | Excel report output |
| **scipy / numpy** | Statistical anomaly detection |
| **Power BI Desktop** | Interactive dashboard |
| **SQL** | 3-layer transformation architecture |
| **PyYAML** | Configuration management |
| **pytest** | Unit testing |

---

## 📁 Outputs

After running the pipeline, find outputs in `data/processed/`:

| File | Description |
|---|---|
| `report_summary.xlsx` | Auto-generated Excel workbook (Revenue, Regional, Anomaly tabs) |
| `anomalies.csv` | Flagged transactions with Z-scores |
| `pipeline.db` | SQLite database with all transformation layers |

---

## 📄 License

MIT License — free to use, modify, and distribute.

---

## 👤 Author

Built as a portfolio project demonstrating enterprise-grade data engineering and BI automation skills.

> **Skills demonstrated:** SQL (DDL/DML/Views/Stored Procs), Python (ETL pipelines), Power BI (multi-page dashboards), Data Architecture (medallion pattern), Statistical Analysis (anomaly detection), Software Engineering (config-driven, tested, logged)
