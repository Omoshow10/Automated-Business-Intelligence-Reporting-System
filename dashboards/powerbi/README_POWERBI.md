# 📊 Power BI Dashboard Documentation

**File:** `BI_Reporting_System.pbix`  
**Data Source:** SQLite database (`data/pipeline.db`) via ODBC or direct connector  
**Last Updated:** After each pipeline run

---

## Setup: Connect Power BI to the SQLite Database

### Option A — SQLite ODBC Driver (Recommended)
1. Install the [SQLite ODBC Driver](http://www.ch-werner.de/sqliteodbc/)
2. In Power BI Desktop: **Get Data → ODBC**
3. DSN: `SQLite3 Datasource` → Database path: `<your-path>/data/pipeline.db`
4. Select all `rpt_*` tables and views (`vw_*`)

### Option B — Import CSV Outputs
If ODBC is unavailable, import the processed CSV files:
- `data/processed/report_summary.xlsx` (all tabs)
- `data/processed/anomalies.csv`

### Refresh Schedule
- **Manual:** Click **Refresh** in Power BI Desktop after running `run_pipeline.py`
- **Automated:** Publish to Power BI Service and configure scheduled refresh

---

## Dashboard Pages

---

### Page 1 — Executive Summary

**Purpose:** C-suite view of overall business health.

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Total Revenue | KPI Card | `rpt_monthly_revenue` | `SUM(total_revenue)` |
| Gross Profit | KPI Card | `rpt_monthly_revenue` | `SUM(total_gross_profit)` |
| YoY Revenue Growth | KPI Card | `vw_revenue_trends` | Last 12 months vs prior |
| Profit Margin % | KPI Card | `core_sales` | `AVG(profit_margin) * 100` |
| Revenue Trend | Line Chart | `vw_revenue_trends` | `total_revenue` by `month_label` |
| Profit Margin Trend | Area Chart | `vw_revenue_trends` | `profit_margin_pct` by month |
| Revenue by Segment | Donut Chart | `core_sales` | `SUM(revenue)` by `customer_segment` |
| Revenue by Channel | Clustered Bar | `core_sales` | `SUM(revenue)` by `channel`, year |

**Key DAX Measures:**
```dax
Total Revenue = SUM(rpt_monthly_revenue[total_revenue])

Gross Profit = SUM(rpt_monthly_revenue[total_gross_profit])

Profit Margin % = 
    DIVIDE(
        SUM(core_sales[gross_profit]),
        SUM(core_sales[revenue]),
        0
    ) * 100

YoY Growth % = 
VAR CurrentYear = MAX(core_sales[txn_year])
VAR CurrentRev  = CALCULATE(SUM(core_sales[revenue]), core_sales[txn_year] = CurrentYear)
VAR PriorRev    = CALCULATE(SUM(core_sales[revenue]), core_sales[txn_year] = CurrentYear - 1)
RETURN
    IF(PriorRev = 0, BLANK(), DIVIDE(CurrentRev - PriorRev, PriorRev) * 100)

MoM Growth % = 
VAR CurrentMonth = SELECTEDVALUE(vw_revenue_trends[month_label])
RETURN
    CALCULATE(
        AVERAGE(vw_revenue_trends[mom_growth_pct]),
        vw_revenue_trends[month_label] = CurrentMonth
    )
```

**Filters / Slicers:**
- Year slicer (2022 / 2023 / 2024)
- Customer Segment multi-select
- Channel multi-select

---

### Page 2 — Regional Performance

**Purpose:** Geographic breakdown of revenue and profitability.

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Revenue by Region | Horizontal Bar | `vw_regional_performance` | `total_revenue` by `region` |
| Profit Margin by Region | Column Chart | `vw_regional_performance` | `profit_margin_pct` by `region` |
| Regional Growth Matrix | Matrix Table | `vw_regional_performance` | `total_revenue`, `yoy_revenue_growth_pct` |
| Geography Map | Filled Map | `vw_regional_performance` | Revenue by `region` |
| Top Sales Rep per Region | Table | `vw_regional_performance` | `top_sales_rep`, `top_rep_revenue` |
| Revenue Share Donut | Donut | `vw_regional_performance` | `region_revenue_share_pct` |

**Key DAX Measures:**
```dax
Regional Revenue Share = 
    DIVIDE(
        SUM(core_sales[revenue]),
        CALCULATE(SUM(core_sales[revenue]), ALL(core_sales[region]))
    ) * 100

Regional YoY Growth =
VAR SelRegion = SELECTEDVALUE(core_sales[region])
VAR CurrYear  = MAX(core_sales[txn_year])
VAR CurrRev   = CALCULATE(SUM(core_sales[revenue]),
                    core_sales[region] = SelRegion,
                    core_sales[txn_year] = CurrYear)
VAR PriorRev  = CALCULATE(SUM(core_sales[revenue]),
                    core_sales[region] = SelRegion,
                    core_sales[txn_year] = CurrYear - 1)
RETURN IF(PriorRev = 0, BLANK(), DIVIDE(CurrRev - PriorRev, PriorRev) * 100)

Avg Transaction Value =
    DIVIDE(SUM(core_sales[revenue]), COUNT(core_sales[transaction_id]))
```

**Conditional Formatting:**
- Growth % column: red if negative, green if positive
- Profit margin column: gradient from red (< 20%) to green (> 50%)

---

### Page 3 — Product & Channel Analysis

**Purpose:** Understand product mix, category performance, and channel efficiency.

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Revenue by Category | Treemap | `rpt_product_summary` | `total_revenue` by `product_category` |
| Top 10 Products | Bar Chart | `rpt_product_summary` | `total_revenue` Top N filter = 10 |
| Channel Mix Over Time | Stacked Bar | `core_sales` | `SUM(revenue)` by `channel`, `txn_month_label` |
| Discount vs Profit | Scatter Plot | `core_sales` | X=`discount_pct`, Y=`profit_margin`, Size=`revenue` |
| Category Margin Comparison | Column | `rpt_product_summary` | `avg_profit_margin` by `product_category` |
| Units Sold by Product | Bar | `rpt_product_summary` | `units_sold` Top 15 |

**Key DAX Measures:**
```dax
Category Revenue Share = 
    DIVIDE(
        SUM(rpt_product_summary[total_revenue]),
        CALCULATE(SUM(rpt_product_summary[total_revenue]), ALL(rpt_product_summary[product_category]))
    ) * 100

Avg Discount % =
    AVERAGE(core_sales[discount_pct])

Discount Impact =
    SUMX(
        core_sales,
        core_sales[discount_amount]
    )

Revenue per Unit = 
    DIVIDE(SUM(core_sales[revenue]), SUM(core_sales[units_sold]))
```

**Filters / Slicers:**
- Product Category multi-select
- Year slicer
- Channel slicer

---

### Page 4 — Anomaly Detection & Ops Metrics

**Purpose:** Surface statistical outliers and operational red flags for investigation.

**Visuals:**

| Visual | Type | Data Source | Measure |
|---|---|---|---|
| Anomaly Flags Count | KPI Card | `rpt_anomalies` | `COUNT(transaction_id)` |
| Revenue Confidence Band | Line + Area | `vw_anomaly_detection` | `total_revenue` + `upper/lower_bound_2sd` |
| Anomalies by Type | Donut | `rpt_anomalies` | Count by `anomaly_type` |
| Anomaly Timeline | Scatter | `rpt_anomalies` | `txn_date` vs `revenue`, color by type |
| Flagged Transactions Table | Table | `rpt_anomalies` | All columns, sorted by `revenue_zscore` |
| Anomalous Revenue by Month | Column | `vw_anomaly_detection` | `anomalous_revenue` by `month_label` |
| Sales Rep Outliers | Bar | `core_sales` | Z-score of rep revenue vs avg |

**Key DAX Measures:**
```dax
Anomaly Count = 
    COUNTROWS(rpt_anomalies)

Anomalous Revenue % = 
    DIVIDE(
        SUM(rpt_anomalies[revenue]),
        CALCULATE(SUM(core_sales[revenue]), ALL(core_sales))
    ) * 100

Revenue Upper Band = 
    AVERAGE(vw_anomaly_detection[upper_bound_2sd])

Revenue Lower Band = 
    AVERAGE(vw_anomaly_detection[lower_bound_2sd])
```

**Conditional Formatting on Flagged Table:**
- `revenue_zscore` > 2.5: red background
- `revenue_zscore` < -2.5: orange background
- `anomaly_type` = "HIGH_REVENUE": bold font

---

## Data Model Relationships

```
core_sales (fact)
    ├── [txn_month_label] → rpt_monthly_revenue [month_label]     (many:1)
    ├── [region, txn_year] → rpt_regional_summary [region, year]  (many:1)
    ├── [product_name, txn_year] → rpt_product_summary [...]      (many:1)
    └── [transaction_id] → rpt_anomalies [transaction_id]         (1:0..1)

vw_revenue_trends ← derived from rpt_monthly_revenue
vw_regional_performance ← derived from rpt_regional_summary + core_sales
vw_anomaly_detection ← derived from core_sales + rpt_anomalies
```

---

## Refresh & Publishing

```bash
# Step 1: Run the full pipeline
python run_pipeline.py

# Step 2: In Power BI Desktop, click:
# Home → Refresh

# Step 3: Publish to Power BI Service:
# File → Publish → Publish to Power BI
```

After publishing, configure **Scheduled Refresh** in Power BI Service
(Datasets → Settings → Scheduled Refresh) to match your pipeline run frequency.

---

## Troubleshooting

| Issue | Fix |
|---|---|
| "Cannot find table" error | Re-run `run_pipeline.py --stage transform` to recreate tables |
| SQLite ODBC not connecting | Verify ODBC driver installed; check DB path in connection string |
| Blank visuals after refresh | Check that pipeline completed without errors in `logs/` |
| Map visual shows no data | Verify region names match Power BI geography recognition |
| Stale data after pipeline run | Click **Refresh** in Power BI Desktop or re-publish |
