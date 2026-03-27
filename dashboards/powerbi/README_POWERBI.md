# 📊 Power BI Dashboard Documentation

**File:** `BI_Reporting_System.pbix`
**Platform:** MS SQL Server 2019+ (T-SQL) via native SQL Server connector
**Author:** Olayinka Somuyiwa
**Last Updated:** After each automated pipeline run (daily at 06:30)

---

## Overview

Power BI is the **reporting output layer** of the automated BI pipeline. It connects directly to MS SQL Server (`bi_reporting_db`), reads from the pre-aggregated `dbo.rpt_*` tables and `dbo.vw_*` analytical views produced by the T-SQL transformation layers, and refreshes automatically on a scheduled basis — no manual intervention required.

```
MS SQL Server (bi_reporting_db)
    dbo.core_sales
    dbo.rpt_monthly_revenue
    dbo.rpt_regional_summary
    dbo.rpt_product_summary
    dbo.rpt_anomalies
    dbo.pipeline_log
    dbo.vw_revenue_trends
    dbo.vw_regional_performance
    dbo.vw_anomaly_detection
    dbo.vw_product_analysis
         │
         └──▶  Power BI Desktop (BI_Reporting_System.pbix)
                    │
                    └──▶  Power BI Service (scheduled refresh daily at 07:00)
```

---

## Setup: Connect Power BI to MS SQL Server

### Step 1 — Open the Report
Open `BI_Reporting_System.pbix` in **Power BI Desktop** (free download from Microsoft).

### Step 2 — Update the Data Source Connection
1. **Home → Transform Data → Data Source Settings**
2. Select the SQL Server source → **Change Source**
3. Set **Server:** `your_server` (match `config/config.yaml`)
4. Set **Database:** `bi_reporting_db`
5. Click **OK**

### Step 3 — Authenticate
- **Windows Authentication** — select *Use my current credentials* (matches `trusted_connection: true` in config)
- **SQL Server Authentication** — enter the username and password configured in `config/config.yaml`

### Step 4 — Select Tables and Views
On first connection, import all of the following from the `dbo` schema:

| Object | Type | Used On |
|---|---|---|
| `core_sales` | Table | All pages |
| `rpt_monthly_revenue` | Table | Page 1, 3 |
| `rpt_regional_summary` | Table | Page 2 |
| `rpt_product_summary` | Table | Page 3 |
| `rpt_anomalies` | Table | Page 4 |
| `pipeline_log` | Table | Page 2 |
| `vw_revenue_trends` | View | Page 1 |
| `vw_regional_performance` | View | Page 2 |
| `vw_anomaly_detection` | View | Page 4 |
| `vw_product_analysis` | View | Page 3 |

### Step 5 — Refresh
Click **Home → Refresh** to load data from the live SQL Server database.

---

## Dashboard Pages

### Page 1 — Executive Overview Dashboard

**Purpose:** Automated KPI summary with data freshness timestamp, pipeline status indicator, and key operational metrics.

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Total Revenue | KPI Card | `rpt_monthly_revenue` | `SUM(total_revenue)` |
| Gross Profit | KPI Card | `rpt_monthly_revenue` | `SUM(total_gross_profit)` |
| YoY Revenue Growth % | KPI Card | `vw_revenue_trends` | `yoy_growth_pct` latest month |
| Profit Margin % | KPI Card | `core_sales` | `gross_profit / revenue * 100` |
| Monthly Revenue Trend | Line Chart | `vw_revenue_trends` | `total_revenue` by `month_label` — 2022/2023/2024 |
| Profit Margin Trend | Area Chart | `vw_revenue_trends` | `profit_margin_pct` by `month_label` |
| Revenue by Customer Segment | Donut Chart | `core_sales` | `SUM(revenue)` by `customer_segment` |
| Revenue by Channel | Horizontal Bar | `core_sales` | `SUM(revenue)` by `channel` |
| Revenue by Region | Clustered Bar | `rpt_regional_summary` | `total_revenue` by `region` |
| Pipeline Status | Table | `dbo.pipeline_log` | `stage`, `status`, `records_processed`, `duration_seconds` — today |

**Key DAX Measures:**
```dax
Total Revenue =
    SUM(rpt_monthly_revenue[total_revenue])

Gross Profit =
    SUM(rpt_monthly_revenue[total_gross_profit])

Profit Margin % =
    DIVIDE(
        SUM(core_sales[gross_profit]),
        SUM(core_sales[revenue]),
        0
    ) * 100

YoY Growth % =
VAR CurrentYear = MAX(core_sales[txn_year])
VAR CurrentRev  = CALCULATE(SUM(core_sales[revenue]),
                      core_sales[txn_year] = CurrentYear)
VAR PriorRev    = CALCULATE(SUM(core_sales[revenue]),
                      core_sales[txn_year] = CurrentYear - 1)
RETURN
    IF(PriorRev = 0, BLANK(), DIVIDE(CurrentRev - PriorRev, PriorRev) * 100)

MoM Growth % =
VAR CurrentMonth = SELECTEDVALUE(vw_revenue_trends[month_label])
RETURN
    CALCULATE(
        AVERAGE(vw_revenue_trends[mom_growth_pct]),
        vw_revenue_trends[month_label] = CurrentMonth
    )

Data Freshness =
    "Data as of: " & FORMAT(MAX(core_sales[txn_date]), "YYYY-MM-DD")

Pipeline Status Today =
    CALCULATE(
        MAX(pipeline_log[status]),
        pipeline_log[run_date] = TODAY()
    )
```

**Slicers:** Year (2022 / 2023 / 2024) · Customer Segment · Channel

---

### Page 2 — Pipeline Run Log & Monitoring Panel

**Purpose:** Pipeline execution log showing daily run timestamps, records processed per run, processing duration, and status (Completed / Failed).

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Total Runs (30d) | KPI Card | `dbo.pipeline_log` | `COUNT(log_id)` last 30 days |
| Successful Runs | KPI Card | `dbo.pipeline_log` | `COUNTROWS` where `status = 'Completed'` |
| Failed Runs | KPI Card | `dbo.pipeline_log` | `COUNTROWS` where `status = 'Failed'` |
| Avg Duration | KPI Card | `dbo.pipeline_log` | `AVERAGE(duration_seconds)` |
| Avg Records/Run | KPI Card | `dbo.pipeline_log` | `AVERAGE(records_processed)` |
| Run Duration Trend | Bar Chart | `dbo.pipeline_log` | `duration_seconds` by `run_date` — failed runs highlighted red |
| Records Processed Trend | Line Chart | `dbo.pipeline_log` | `records_processed` by `run_date` |
| Stage Avg Duration | Horizontal Bar | `dbo.pipeline_log` | `AVERAGE(duration_seconds)` by `stage` |
| 30-Day Success Rate | Donut | `dbo.pipeline_log` | Completed vs Failed count |
| Stage Success Summary | Matrix | `dbo.pipeline_log` | Runs / Success / Failed by `stage` |
| Full Run Log | Table | `dbo.pipeline_log` | All columns — sorted by `log_id` DESC |

**Key DAX Measures:**
```dax
Success Rate % =
    DIVIDE(
        CALCULATE(COUNTROWS(pipeline_log), pipeline_log[status] = "Completed"),
        COUNTROWS(pipeline_log),
        0
    ) * 100

Failed Runs =
    CALCULATE(COUNTROWS(pipeline_log), pipeline_log[status] = "Failed")

Avg Duration (s) =
    AVERAGE(pipeline_log[duration_seconds])

Run Status Color =
    IF(SELECTEDVALUE(pipeline_log[status]) = "Completed", "#375623", "#C00000")
```

**Conditional Formatting:**
- `status = 'Failed'` rows: red background in the run log table
- `duration_seconds` bar: red fill for any run under 2 seconds (indicates early failure)

**Slicers:** Date range · Stage · Status

---

### Page 3 — Automated Report Output — Management Report

**Purpose:** Auto-generated management report showing formatted KPI tables, trend charts, and summary outputs produced without manual intervention.

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Annual KPI Table | Matrix | `rpt_monthly_revenue`, `core_sales` | Revenue / Profit / Margin / Transactions by year |
| Quarterly Revenue Comparison | Clustered Bar | `rpt_monthly_revenue` | `total_revenue` by quarter — 2022/2023/2024 |
| Regional Performance Table | Matrix | `rpt_regional_summary` | Revenue / YoY / Margin / Share by region |
| Revenue by Product Category | Horizontal Bar | `rpt_product_summary` | `total_revenue` by `product_category` |
| Profit Margin by Channel | Column Chart | `core_sales` | `AVG(profit_margin)` by `channel` |
| Top 5 Products Table | Table | `rpt_product_summary` | `product_name`, `total_revenue`, `total_gross_profit`, `avg_profit_margin`, `units_sold` |

**Key DAX Measures:**
```dax
Revenue by Quarter =
    CALCULATE(
        SUM(rpt_monthly_revenue[total_revenue]),
        FILTER(
            rpt_monthly_revenue,
            rpt_monthly_revenue[txn_month] IN {1,2,3}   -- adjust per quarter
        )
    )

Regional Revenue Share =
    DIVIDE(
        SUM(rpt_regional_summary[total_revenue]),
        CALCULATE(
            SUM(rpt_regional_summary[total_revenue]),
            ALL(rpt_regional_summary[region])
        )
    ) * 100

Top N Products Revenue =
    CALCULATE(
        SUM(rpt_product_summary[total_revenue]),
        TOPN(5, ALL(rpt_product_summary[product_name]),
             SUM(rpt_product_summary[total_revenue]))
    )

Revenue per Unit =
    DIVIDE(SUM(core_sales[revenue]), SUM(core_sales[units_sold]))

Avg Discount % =
    AVERAGE(core_sales[discount_pct])
```

**Report Generation Stamp** (Text card visual on page):
> *This report was generated automatically by the BI Pipeline · Source: dbo.rpt_monthly_revenue · dbo.rpt_regional_summary · dbo.rpt_product_summary · No manual intervention*

**Slicers:** Year · Product Category · Channel · Customer Segment

---

### Page 4 — Data Quality Validation Report

**Purpose:** Automated data quality check output — null value counts, validation pass/fail status, and data completeness metrics per pipeline run.

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Overall Quality Score | KPI Card | `dbo.pipeline_log` | Rows loaded / (rows loaded + rejected) * 100 |
| Rows Loaded | KPI Card | `dbo.pipeline_log` | `SUM(records_processed)` where stage = 'Staging' |
| Rows Rejected | KPI Card | Calculated | Total rows − loaded rows |
| Issues Auto-Fixed | KPI Card | Calculated | Clamped discount + null unit fixes |
| Duplicates Removed | KPI Card | Calculated | From deduplication CTE in staging |
| Validation Check Results | Table | `dbo.pipeline_log` + Python output | Check name / rows affected / action / result |
| Null Count by Column | Bar Chart | Calculated | Null counts per column — green = 0, orange = has nulls |
| Data Completeness % | Horizontal Bar | Calculated | Completeness per column |
| Quality Score Trend (30d) | Line + Bar | `dbo.pipeline_log` | Quality score line + rejected rows bar |
| Staging Log Extract | Table | `dbo.pipeline_log` | `run_date`, `status`, `records_processed`, `message` — stage = 'Staging' |

**Key DAX Measures:**
```dax
Quality Score % =
    DIVIDE(
        CALCULATE(
            SUM(pipeline_log[records_processed]),
            pipeline_log[stage] = "Staging",
            pipeline_log[status] = "Completed"
        ),
        CALCULATE(
            SUM(pipeline_log[records_processed]),
            pipeline_log[stage] = "Staging"
        ) + [Rows Rejected],
        0
    ) * 100

Staging Success =
    CALCULATE(
        COUNTROWS(pipeline_log),
        pipeline_log[stage] = "Staging",
        pipeline_log[status] = "Completed"
    )

Pipeline Failures (30d) =
    CALCULATE(
        COUNTROWS(pipeline_log),
        pipeline_log[status] = "Failed",
        pipeline_log[run_date] >= TODAY() - 30
    )
```

**Conditional Formatting:**
- Validation result = `PASS` → green cell background
- Validation result = `WARN` → orange cell background
- Validation result = `FIXED` → blue cell background
- `status = 'Failed'` in log table → red row background

**Slicers:** Date range · Stage · Status

---

## Data Model Relationships

```
dbo.core_sales  (fact table)
    ├── [txn_month_label] → dbo.rpt_monthly_revenue [month_label]       many:1
    ├── [region, txn_year] → dbo.rpt_regional_summary [region, txn_year] many:1
    ├── [product_name, txn_year] → dbo.rpt_product_summary              many:1
    └── [transaction_id] → dbo.rpt_anomalies [transaction_id]           1:0..1

dbo.vw_revenue_trends       ← derived from dbo.rpt_monthly_revenue
                               adds: mom_growth_pct, yoy_growth_pct, ytd_revenue
                               uses: LAG() window function (T-SQL)

dbo.vw_regional_performance ← derived from dbo.rpt_regional_summary + dbo.core_sales
                               adds: yoy_revenue_growth_pct, region_revenue_share_pct,
                                     avg_transaction_value, top_sales_rep
                               uses: RANK() window function (T-SQL)

dbo.vw_anomaly_detection    ← derived from dbo.core_sales + dbo.rpt_anomalies
                               adds: lower_bound_2sd, upper_bound_2sd, anomaly_count
                               uses: STDEV() aggregate (T-SQL)

dbo.vw_product_analysis     ← derived from dbo.core_sales
                               adds: rank_in_category, overall_rank, category_share_pct
                               uses: RANK() window function (T-SQL)

dbo.pipeline_log            ← standalone audit table (not joined to sales fact)
                               written to by: usp_DailyDataExtraction,
                                              usp_TransformationLayer,
                                              pipeline_runner.py
```

---

## Refresh & Publishing

### Manual Refresh (Power BI Desktop)

```
1. Run the pipeline:   python run_pipeline.py
2. Open Power BI Desktop
3. Home → Refresh
```

### Automated Refresh (Power BI Service)

```
1. Publish:  File → Publish → Publish to Power BI
2. In Power BI Service:
   Datasets → bi_reporting_db → Settings → Scheduled Refresh
3. Configure:
   - Frequency:   Daily
   - Time:        07:00  (30 min after pipeline completes at 06:30)
   - Time zone:   match your SQL Server time zone
4. Configure gateway:
   On-premises data gateway required for SQL Server connectivity
```

### Pipeline → Power BI Timing

```
06:00  usp_DailyDataExtraction  (SQL Server Agent)
06:30  pipeline_runner.py        (Python scheduler — transformation + reporting)
06:32  Excel report auto-generated to data/processed/
07:00  Power BI Service refresh  (reads from bi_reporting_db)
07:02  Dashboards updated        (all 4 pages reflect latest data)
```

---

## Troubleshooting

| Issue | Cause | Fix |
|---|---|---|
| "Cannot connect to server" | Wrong server name or firewall | Verify `server` in `config/config.yaml` matches SQL Server instance name |
| "Login failed for user" | Authentication mismatch | Switch between Windows auth and SQL auth in Power BI data source settings |
| "Cannot find table dbo.core_sales" | Schema not created | Run `sql/transformations/01_create_schema.sql` in SSMS then re-run pipeline |
| Blank visuals after refresh | Pipeline did not complete | Check `SELECT * FROM dbo.pipeline_log ORDER BY log_id DESC` — look for 'Failed' status |
| Map visual shows no data | Region names not recognised | Verify regions in `core_sales` match Power BI geography: North/South/East/West/International |
| Stale data in dashboard | Refresh not triggered | Click **Refresh** in Power BI Desktop or wait for scheduled 07:00 refresh |
| "Cannot find column duration_seconds" | Computed column not returning | Verify `start_time` and `end_time` are both populated in `dbo.pipeline_log` |
| Gateway connection error | Gateway not configured | Install On-premises data gateway; configure connection to `bi_reporting_db` |
