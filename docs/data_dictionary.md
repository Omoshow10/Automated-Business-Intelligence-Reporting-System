# Data Dictionary

## Source: `data/raw/sales_operations.csv`

| Column | Type | Example | Description |
|---|---|---|---|
| `transaction_id` | STRING | TXN-000001 | Unique transaction identifier |
| `date` | DATE | 2023-06-15 | Transaction date (ISO 8601) |
| `product_name` | STRING | Laptop Pro 15 | Full product name |
| `product_category` | STRING | Electronics | Electronics / Software / Services / Hardware |
| `region` | STRING | North | North / South / East / West / International |
| `sales_rep` | STRING | Alex Johnson | Full name of the sales representative |
| `customer_id` | STRING | CUST-1001 | Customer identifier (not unique per transaction) |
| `customer_segment` | STRING | Enterprise | Enterprise / SMB / Consumer |
| `channel` | STRING | Direct | Direct / Partner / Online |
| `units_sold` | INTEGER | 2 | Number of units in this transaction |
| `unit_price` | FLOAT | 1200.00 | List price per unit (USD, pre-discount) |
| `revenue` | FLOAT | 2160.00 | Actual revenue received after discount |
| `cost` | FLOAT | 1400.00 | Cost of goods sold for this transaction |
| `discount_pct` | FLOAT | 10.0 | Discount applied as a percentage (0–40%) |

---

## Derived: `core_sales` (SQL Core Layer)

All columns from source plus:

| Column | Type | Formula | Description |
|---|---|---|---|
| `txn_date` | DATE | cast of `date` | Typed transaction date |
| `txn_year` | INTEGER | `YEAR(txn_date)` | Calendar year (2022–2024) |
| `txn_quarter` | INTEGER | `(month-1)/3 + 1` | Fiscal quarter (1–4) |
| `txn_month` | INTEGER | `MONTH(txn_date)` | Calendar month (1–12) |
| `txn_month_label` | STRING | `2023-06` | Year-month label for time series joins |
| `gross_profit` | FLOAT | `revenue - cost` | Gross profit per transaction |
| `profit_margin` | FLOAT | `gross_profit / revenue` | Profit margin as decimal ratio (0–1) |
| `discount_amount` | FLOAT | `unit_price × units × (discount_pct/100)` | Dollar value of discount applied |
| `is_profitable` | INTEGER | `1 if gross_profit > 0` | Binary profitability flag |

---

## Reporting Tables

### `rpt_monthly_revenue`
Pre-aggregated monthly revenue for dashboard trend charts.

| Column | Description |
|---|---|
| `month_label` | Year-month key (PK) |
| `txn_year` | Calendar year |
| `txn_month` | Calendar month (1–12) |
| `total_revenue` | Sum of revenue |
| `total_cost` | Sum of cost |
| `total_gross_profit` | Sum of gross profit |
| `avg_profit_margin` | Average profit margin ratio |
| `transaction_count` | Number of transactions |
| `units_sold` | Total units |

### `rpt_regional_summary`
Revenue and profitability by region and year.

| Column | Description |
|---|---|
| `region` | Region name (PK with txn_year) |
| `txn_year` | Calendar year (PK with region) |
| `total_revenue` | Sum of revenue |
| `total_cost` | Sum of cost |
| `total_gross_profit` | Sum of gross profit |
| `avg_profit_margin` | Average margin ratio |
| `transaction_count` | Number of transactions |

### `rpt_anomalies`
Transactions flagged as statistical outliers.

| Column | Description |
|---|---|
| `transaction_id` | Source transaction (PK, FK to core_sales) |
| `txn_date` | Date of flagged transaction |
| `product_name` | Product involved |
| `region` | Region of the transaction |
| `revenue` | Raw revenue value |
| `cost` | Raw cost value |
| `revenue_zscore` | Z-score of revenue within product_category |
| `cost_zscore` | Z-score of cost within product_category |
| `anomaly_type` | HIGH_REVENUE / LOW_REVENUE / HIGH_COST / DISCOUNT_OUTLIER |
| `detected_at` | Timestamp when anomaly was flagged |

---

## Analytical Views

### `vw_revenue_trends`
Adds MoM and YoY growth percentages to monthly revenue data. Used on Power BI Page 1.

Key added columns:
- `mom_growth_pct` — month-over-month revenue growth %
- `yoy_growth_pct` — year-over-year revenue growth %
- `ytd_revenue` — running YTD total, resets each January

### `vw_regional_performance`
Enriches regional summary with YoY comparisons, revenue share, avg transaction value, and top sales rep. Used on Power BI Page 2.

### `vw_product_analysis`
Product-level detail with category share and overall ranking. Used on Power BI Page 3.

### `vw_anomaly_detection`
Monthly statistics with expected revenue bands (±2σ) for band chart. Used on Power BI Page 4.

---

## Business Definitions

| Term | Definition |
|---|---|
| **Revenue** | Actual amount invoiced to the customer after all discounts |
| **Cost** | Direct cost of goods/services for the transaction |
| **Gross Profit** | Revenue − Cost (before operating expenses) |
| **Profit Margin** | Gross Profit ÷ Revenue, expressed as a percentage |
| **YoY Growth** | (Current Year Revenue − Prior Year Revenue) ÷ Prior Year Revenue |
| **MoM Growth** | (Current Month Revenue − Prior Month Revenue) ÷ Prior Month Revenue |
| **Anomaly** | Transaction where |Z-score| > 2.5σ from the product category mean |
