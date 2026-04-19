# System Architecture

## Overview

The Automated BI Reporting System follows a classic **ELT (Extract, Load, Transform)** pattern with a layered SQL architecture, orchestrated by a Python pipeline runner.

```
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                      │
│                                                                      │
│   sales_operations.csv   (10,800 rows, 3 years of daily sales)      │
│   [In production: ERP / CRM / data warehouse API feeds]             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 1 — DATA INGESTION                python/ingestion/          │
│                                                                      │
│  ▸ Schema validation (required columns, types)                      │
│  ▸ Data quality checks (nulls, negatives, duplicates)               │
│  ▸ Auto-fix minor issues (clamp discount, fill nulls)               │
│  ▸ Bulk load → stg_sales_raw table                                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  STAGE 2 — SQL TRANSFORMATION LAYERS     sql/transformations/       │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  STAGING LAYER        stg_sales_raw                         │    │
│  │  • Mirrors raw CSV structure                                │    │
│  │  • Deduplication, standardization of categoricals          │    │
│  └───────────────────────────┬─────────────────────────────────┘    │
│                              │                                       │
│  ┌───────────────────────────▼─────────────────────────────────┐    │
│  │  CORE LAYER           core_sales                            │    │
│  │  • Typed fields (date decomposition: year, quarter, month)  │    │
│  │  • Derived: gross_profit, profit_margin, discount_amount    │    │
│  │  • Business flags: is_profitable                            │    │
│  └───────────────────────────┬─────────────────────────────────┘    │
│                              │                                       │
│  ┌───────────────────────────▼─────────────────────────────────┐    │
│  │  REPORTING LAYER      rpt_monthly_revenue                   │    │
│  │                       rpt_regional_summary                  │    │
│  │                       rpt_product_summary                   │    │
│  │  • Pre-aggregated for dashboard performance                 │    │
│  │  • Full refresh on each pipeline run                        │    │
│  └───────────────────────────┬─────────────────────────────────┘    │
│                              │                                       │
│  ┌───────────────────────────▼─────────────────────────────────┐    │
│  │  ANALYTICAL VIEWS     vw_revenue_trends                     │    │
│  │                       vw_regional_performance               │    │
│  │                       vw_product_analysis                   │    │
│  │                       vw_anomaly_detection                  │    │
│  │  • Window functions (MoM, YoY, running totals)              │    │
│  │  • Ranking and market share calculations                    │    │
│  └─────────────────────────────────────────────────────────────┘    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │                     │
                    ▼                     ▼
┌───────────────────────────┐  ┌───────────────────────────────────────┐
│  STAGE 3a — ANOMALY        │  │  STAGE 3b — REPORT GENERATION         │
│  DETECTION                 │  │                                        │
│  python/reporting/         │  │  python/reporting/                     │
│                            │  │                                        │
│  ▸ Z-score detection       │  │  ▸ Excel workbook (5 tabs)             │
│    (per product_category)  │  │    • Executive Summary                 │
│  ▸ IQR detection           │  │    • Monthly Trends                    │
│    (discount outliers)     │  │    • Regional Performance              │
│  ▸ Write → rpt_anomalies   │  │    • Product Analysis                  │
│  ▸ Export anomalies.csv    │  │    • Anomaly Report                    │
└───────────────────────────┘  └───────────────────────────────────────┘
                    │                     │
                    └──────────┬──────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│  POWER BI DASHBOARD          dashboards/powerbi/                     │
│                                                                      │
│  Page 1 — Executive Summary   (KPIs, revenue trend, segment mix)    │
│  Page 2 — Regional Performance (maps, YoY, rep leaderboard)         │
│  Page 3 — Product & Channel   (treemap, scatter, channel mix)       │
│  Page 4 — Anomaly Detection   (band chart, flagged transactions)    │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Design Decisions

### Medallion Architecture (Staging → Core → Reporting)
The 3-layer SQL approach cleanly separates concerns:
- **Staging** catches data quality issues early without corrupting downstream data
- **Core** is the single source of truth - business logic lives here, not in reports
- **Reporting** pre-aggregates heavy queries so Power BI responds instantly

### Full Refresh vs Incremental
This pipeline uses **full refresh** (truncate and reload) for simplicity. In a production environment with millions of rows, you would replace this with incremental logic using a `loaded_at` watermark column in the staging table.

### SQLite for Local Dev, SQL Server for Production
The `DBConnector` class abstracts the database engine. Switching from SQLite to SQL Server requires only a `config.yaml` change - all SQL files use ANSI-compatible syntax.

### Statistical Anomaly Detection
Z-scores are computed **within product_category groups** rather than globally, because a $60,000 server sale is not anomalous but a $60,000 webcam sale is. IQR detection catches discount outliers that Z-score may miss due to non-normality.

---

## Scalability Path

| Scale | Approach |
|---|---|
| < 1M rows | SQLite (current setup) |
| 1M – 100M rows | SQL Server / PostgreSQL |
| 100M+ rows | Snowflake / BigQuery + dbt for transformations |
| Real-time | Apache Kafka → Spark Streaming → Data Lakehouse |
