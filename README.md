# 📊 Automated Business Intelligence Reporting System

> \*\*Enterprise Data Reporting Automation\*\* - End-to-end automated data pipeline demonstrating scalable BI infrastructure using MS SQL Server (T-SQL), Python, Power BI, and Excel across financial and healthcare reporting environments.

**Author:** Olayinka Somuyiwa  
**GitHub:** [Automated-Business-Intelligence-Reporting-System](https://github.com/Omoshow10/Automated-Business-Intelligence-Reporting-System)  
**Status:** Complete - publicly available

\---

## 🎯 Project Overview

This project demonstrates a production-grade **automated BI reporting pipeline** that eliminates manual reporting processes by replacing them with reliable, scheduled, automated data pipelines and interactive reporting outputs. The system is designed for cross-sector applicability across U.S. financial and healthcare organizations.

**Core pipeline:**

```
Raw Operational Data → SQL Extraction (T-SQL) → Python Transformation → MS SQL Server → Power BI Dashboard + Excel Reports
```

\---

## ✨ Features

|Feature|Description|
|-|-|
|📥 **Automated Data Extraction**|T-SQL stored procedure (`usp\_DailyDataExtraction`) scheduled via SQL Server Agent at 06:00 daily|
|🔄 **Python Transformation**|`pipeline\_runner.py` - data quality validation, aggregation, and reporting layer output via pyodbc|
|🗄️ **3-Layer SQL Architecture**|Staging → Core → Reporting (medallion pattern) in MS SQL Server|
|📄 **Automated Report Generation**|Excel workbook with 5 tabs auto-generated without manual intervention|
|📊 **Power BI Dashboard**|4-page interactive dashboard: Executive Summary, Regional, Product, Anomaly Detection|
|🚨 **Anomaly Detection**|Z-score and IQR-based statistical outlier detection written to `dbo.rpt\_anomalies`|
|🔁 **Pipeline Logging**|Full run logs written to `dbo.pipeline\_log` - every stage, every run|
|⏰ **Scheduling**|SQL Server Agent (extraction at 06:00) + Python `schedule` library (transformation at 06:30)|

\---

## 🗂️ Project Structure

```
Automated-Business-Intelligence-Reporting-System/
│
├── data/
│   ├── raw/                              # Source CSV files
│   │   └── sales\_operations.csv          # 3-year sales dataset (10,800 rows)
│   └── processed/                        # Pipeline outputs
│       └── report\_summary\_\*.xlsx         # Auto-generated Excel report
│
├── sql/
│   ├── transformations/
│   │   ├── 01\_create\_schema.sql          # MS SQL Server DDL — all tables, indexes
│   │   ├── 02\_staging\_layer.sql          # Staging validation and clean (T-SQL)
│   │   ├── 03\_core\_layer.sql             # Core enrichment — derived metrics (T-SQL)
│   │   └── 04\_reporting\_layer.sql        # Reporting aggregation (T-SQL)
│   ├── views/
│   │   ├── vw\_revenue\_trends.sql         # MoM and YoY growth (T-SQL window functions)
│   │   ├── vw\_regional\_performance.sql   # Regional KPIs with YoY
│   │   └── vw\_anomaly\_detection.sql      # Revenue band + anomaly counts
│   └── stored\_procedures/
│       └── sp\_refresh\_reporting.sql      # usp\_DailyDataExtraction + usp\_TransformationLayer
│
├── python/
│   ├── ingestion/
│   │   └── data\_loader.py                # CSV → dbo.stg\_sales\_raw (pyodbc)
│   ├── transformation/
│   │   ├── pipeline\_runner.py            # Orchestrator with schedule — pyodbc to SQL Server
│   │   └── sql\_runner.py                 # Executes SQL transformation files in sequence
│   ├── reporting/
│   │   ├── report\_generator.py           # Excel report automation (openpyxl)
│   │   └── anomaly\_detector.py           # Z-score + IQR anomaly detection
│   └── utils/
│       ├── db\_connector.py               # MS SQL Server connection manager (pyodbc)
│       ├── logger.py                     # Centralized logging
│       └── config\_loader.py              # YAML config management
│
├── dashboards/
│   └── powerbi/
│       ├── BI\_Reporting\_System.pbix      # Power BI Desktop file
│       └── README\_POWERBI.md             # Dashboard documentation + DAX measures
│
├── outputs/                              # Dashboard screenshots (Fig 20-1 to 20-4)
│   ├── Fig\_20-1\_Executive\_Overview\_Dashboard.png
│   ├── Fig\_20-2\_Pipeline\_Run\_Log\_Monitoring.png
│   ├── Fig\_20-3\_Automated\_Report\_Management.png
│   └── Fig\_20-4\_Data\_Quality\_Validation\_Report.png
│
├── docs/
│   ├── architecture.md                   # System architecture overview
│   ├── data\_dictionary.md                # Column definitions and lineage
│   └── setup\_guide.md                    # Step-by-step setup instructions
│
├── tests/
│   ├── test\_ingestion.py                 # Ingestion validation unit tests
│   ├── test\_transformations.py           # SQL business logic assertions
│   └── test\_anomaly\_detection.py         # Statistical detection edge cases
│
├── config/
│   └── config.yaml                       # Pipeline configuration (SQL Server settings)
│
├── generate\_dataset.py                   # Synthetic sales dataset generator
├── generate\_dashboard\_screenshots.py     # Dashboard PNG generator
├── run\_pipeline.py                       # 🚀 Main pipeline orchestrator
└── requirements.txt
```

\---

## 🏗️ Three-Layer Pipeline Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  EXTRACTION LAYER — usp\_DailyDataExtraction (T-SQL)          │
│  SQL Server Agent Job — daily at 06:00                       │
│  Pulls raw operational data → dbo.staging\_operational\_data   │
└────────────────────────────┬─────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────┐
│  TRANSFORMATION LAYER — pipeline\_runner.py (Python + pyodbc) │
│  Python scheduler — daily at 06:30                           │
│  Data quality validation → aggregation → dbo.reporting\_\*    │
└────────────────────────────┬─────────────────────────────────┘
                             │
┌────────────────────────────▼─────────────────────────────────┐
│  REPORTING LAYER — Power BI + Excel (auto-refresh)           │
│  Power BI reads from dbo.vw\_\* views                          │
│  Excel workbook auto-generated by report\_generator.py        │
└──────────────────────────────────────────────────────────────┘
```

**Core Design Principles:**

* **Repeatable** - pipeline runs re-execute without duplicate or inconsistent outputs
* **Error Handling \& Logging** - every stage writes to `dbo.pipeline\_log` with status, record count, and duration
* **Scalable** - modular architecture; new data sources added without restructuring core pipeline
* **Auditability** - complete data lineage and run history in `dbo.pipeline\_log`
* **Cross-Sector** - ingests and processes data from both financial and healthcare source systems

\---

## 🚀 Quick Start

### Prerequisites

* Python 3.9+
* MS SQL Server 2019+ (or SQL Server Express)
* ODBC Driver for SQL Server
* Power BI Desktop (free from Microsoft)

### 1\. Clone \& Install

```bash
git clone https://github.com/Omoshow10/Automated-Business-Intelligence-Reporting-System.git
cd Automated-Business-Intelligence-Reporting-System
pip install -r requirements.txt
```

### 2\. Create the Database

```sql
-- In SSMS:
CREATE DATABASE bi\_reporting\_db;
GO
```

### 3\. Run Schema Setup

```bash
-- Execute in SSMS against bi\_reporting\_db:
-- sql/transformations/01\_create\_schema.sql
-- sql/stored\_procedures/sp\_refresh\_reporting.sql
-- sql/views/vw\_revenue\_trends.sql
-- sql/views/vw\_regional\_performance.sql
-- sql/views/vw\_anomaly\_detection.sql
```

### 4\. Configure Database Connection

Edit `config/config.yaml`:

```yaml
database:
  server:   YOUR\_SERVER\_NAME
  database: bi\_reporting\_db
  trusted\_connection: true
```

### 5\. Generate Dataset \& Run Pipeline

```bash
python generate\_dataset.py        # Create sales\_operations.csv
python run\_pipeline.py            # Run all 4 stages
```

### 6\. Schedule Automated Runs

The stored procedure `usp\_DailyDataExtraction` is ready for SQL Server Agent scheduling at 06:00. `pipeline\_runner.py` self-schedules transformation at 06:30 via the `schedule` library.

\---

## 📦 Dataset

**File:** `data/raw/sales\_operations.csv`  
**Rows:** 10,800 (3 years daily sales data, 2022–2024)

|Column|Type|Description|
|-|-|-|
|`transaction\_id`|NVARCHAR|Unique transaction identifier|
|`date`|DATE|Transaction date|
|`product\_name`|NVARCHAR|Product sold|
|`product\_category`|NVARCHAR|Electronics / Software / Services / Hardware|
|`region`|NVARCHAR|North / South / East / West / International|
|`sales\_rep`|NVARCHAR|Sales representative name|
|`customer\_segment`|NVARCHAR|Enterprise / SMB / Consumer|
|`revenue`|DECIMAL(14,2)|Transaction revenue (USD)|
|`cost`|DECIMAL(14,2)|Cost of goods sold (USD)|
|`units\_sold`|INT|Number of units|
|`discount\_pct`|DECIMAL(6,2)|Discount applied (0–40%)|
|`channel`|NVARCHAR|Direct / Partner / Online|

\---

## 🗄️ SQL Architecture - MS SQL Server (T-SQL)

All SQL scripts target **MS SQL Server 2019+** using T-SQL syntax:

* `FORMAT()` for date labels, `DATEPART()` for decomposition
* `STDEV()` aggregate for statistical anomaly detection
* `LAG()` window function for MoM comparisons
* `CREATE OR ALTER PROCEDURE` / `CREATE OR ALTER VIEW`
* `IDENTITY`, `BIT`, `NVARCHAR`, `DECIMAL`, `DATETIME2` data types
* SQL Server Agent job setup in `sp\_refresh\_reporting.sql`

**Pipeline stored procedures:**

* `dbo.usp\_DailyDataExtraction` - extracts daily operational data, logs to `dbo.pipeline\_log`
* `dbo.usp\_TransformationLayer` - runs full staging → core → reporting sequence

\---

## 📊 Power BI Dashboard

**File:** `dashboards/powerbi/BI\_Reporting\_System.pbix`  
**Full documentation:** [`dashboards/powerbi/README\_POWERBI.md`](dashboards/powerbi/README_POWERBI.md)

Power BI is the **reporting output layer** of the pipeline. It connects directly to MS SQL Server, reads from the pre-aggregated `rpt\_\*` tables and `vw\_\*` analytical views, and auto-refreshes on a schedule - no manual intervention required.

### Connecting Power BI to MS SQL Server

1. Open `BI\_Reporting\_System.pbix` in **Power BI Desktop**
2. **Home → Transform Data → Data Source Settings**
3. Set server to `your\_server` and database to `bi\_reporting\_db`
4. Select **Windows Authentication** (or SQL Server auth if configured)
5. Click **Refresh** - all 4 pages load from the live database

### Dashboard Pages

#### Page 1 - Executive Summary

Automated KPI summary with data freshness timestamp and pipeline status indicator.

|Visual|Type|Source|
|-|-|-|
|Total Revenue|KPI Card|`rpt\_monthly\_revenue`|
|Gross Profit|KPI Card|`rpt\_monthly\_revenue`|
|YoY Revenue Growth %|KPI Card|`vw\_revenue\_trends`|
|Profit Margin %|KPI Card|`core\_sales`|
|Monthly Revenue Trend|Line Chart|`vw\_revenue\_trends` — MoM and YoY|
|Revenue by Segment|Donut Chart|`core\_sales` — Enterprise / SMB / Consumer|
|Revenue by Channel|Bar Chart|`core\_sales` — Direct / Partner / Online|
|Revenue by Region|Bar Chart|`rpt\_regional\_summary`|
|Pipeline Status Panel|Table|`dbo.pipeline\_log` — live run status|

Key DAX:

```dax
YoY Growth % =
VAR CY = MAX(core\_sales\[txn\_year])
VAR CR = CALCULATE(SUM(core\_sales\[revenue]), core\_sales\[txn\_year] = CY)
VAR PR = CALCULATE(SUM(core\_sales\[revenue]), core\_sales\[txn\_year] = CY - 1)
RETURN IF(PR = 0, BLANK(), DIVIDE(CR - PR, PR) \* 100)

Profit Margin % =
DIVIDE(SUM(core\_sales\[gross\_profit]), SUM(core\_sales\[revenue]), 0) \* 100
```

#### Page 2 - Pipeline Run Log \& Monitoring Panel

Pipeline execution log showing daily run timestamps, records processed per run, processing duration, and status (Completed / Failed).

|Visual|Type|Source|
|-|-|-|
|30-Day Success Rate|Donut|`dbo.pipeline\_log`|
|Run Duration Trend|Bar Chart|`dbo.pipeline\_log` — per run duration|
|Records Processed Trend|Line Chart|`dbo.pipeline\_log` — records\_processed|
|Stage Breakdown|Horizontal Bar|`dbo.pipeline\_log` — avg by stage|
|Full Run Log|Table|`dbo.pipeline\_log` — all columns|
|Stage Success Summary|Matrix|`dbo.pipeline\_log` — grouped by stage|

#### Page 3 - Automated Report Output - Management Report

Auto-generated management report showing formatted KPI tables, trend charts, and summary outputs produced without manual intervention.

|Visual|Type|Source|
|-|-|-|
|Annual KPI Table|Matrix|`rpt\_monthly\_revenue`, `core\_sales`|
|Quarterly Revenue Comparison|Clustered Bar|`rpt\_monthly\_revenue`|
|Regional Performance Table|Matrix|`rpt\_regional\_summary`|
|Revenue by Product Category|Horizontal Bar|`rpt\_product\_summary`|
|Profit Margin by Channel|Column Chart|`core\_sales`|
|Top 5 Products|Table|`rpt\_product\_summary`|

#### Page 4 - Data Quality Validation Report

Automated data quality check output — null value counts, validation pass/fail status, and data completeness metrics per pipeline run.

|Visual|Type|Source|
|-|-|-|
|Overall Quality Score|KPI Card|`dbo.pipeline\_log`|
|Validation Check Results|Table|`dbo.pipeline\_log` + Python output|
|Null Count by Column|Bar Chart|`dbo.pipeline\_log`|
|Data Completeness %|Horizontal Bar|`dbo.pipeline\_log`|
|Quality Score Trend (30d)|Line Chart|`dbo.pipeline\_log`|
|Staging Log Extract|Table|`dbo.pipeline\_log` — stage = 'Staging'|

### Data Model Relationships (Power BI)

```
core\_sales  (fact table)
    ├── \[txn\_month\_label] → rpt\_monthly\_revenue \[month\_label]       many:1
    ├── \[region, txn\_year] → rpt\_regional\_summary \[region, year]    many:1
    ├── \[product\_name, txn\_year] → rpt\_product\_summary              many:1
    └── \[transaction\_id] → rpt\_anomalies \[transaction\_id]           1:0..1

vw\_revenue\_trends      ← derived from rpt\_monthly\_revenue (MoM, YoY, YTD)
vw\_regional\_performance ← derived from rpt\_regional\_summary + core\_sales
vw\_anomaly\_detection   ← derived from core\_sales + rpt\_anomalies
dbo.pipeline\_log       ← standalone (not joined to sales fact)
```

### Scheduled Refresh (Power BI Service)

After publishing to Power BI Service:

1. **Datasets → Settings → Scheduled Refresh**
2. Set to **Daily at 07:00** (30 min after pipeline completes at 06:30)
3. Configure gateway connection to MS SQL Server

\---

## 📸 Dashboard Screenshots

### Executive Overview Dashboard

!\[Executive Overview](outputs/executive\_overview\_dashboard.png)

### Pipeline Run Log \& Monitoring Panel

!\[Pipeline Monitoring](outputs/pipeline\_run\_log\_monitoring.png)

### Automated Management Report

!\[Management Report](outputs/automated\_management\_report.png)

### Data Quality Validation Report

!\[Data Quality](outputs/data\_quality\_validation\_report.png)

\---

## ⚙️ Configuration (`config/config.yaml`)

```yaml
database:
  server:             your\_server
  database:           bi\_reporting\_db
  driver:             SQL Server
  trusted\_connection: true

schedule:
  extraction\_time:     "06:00"    # SQL Server Agent
  transformation\_time: "06:30"    # Python scheduler

anomaly:
  zscore\_threshold: 2.5
  iqr\_multiplier:   1.5
```

\---

## 🧪 Tests

```bash
pytest tests/ -v
```

\---

## 🛠️ Tech Stack

|Tool|Role|
|-|-|
|**MS SQL Server 2019+**|Primary database engine (T-SQL)|
|**Python 3.9+**|Pipeline orchestration, transformation, reporting|
|**pyodbc**|MS SQL Server connection from Python|
|**pandas**|Data manipulation and report generation|
|**openpyxl**|Excel report output (.xlsx)|
|**scipy / numpy**|Statistical anomaly detection (Z-score, IQR)|
|**Power BI Desktop**|Interactive 4-page dashboard|
|**SQL Server Agent**|Automated daily job scheduling|
|**schedule**|Python-side scheduling (06:30 transformation)|
|**PyYAML**|Configuration management|
|**pytest**|Unit testing (35 tests)|

\---

## 📄 License

MIT License — free to use, modify, and distribute.

