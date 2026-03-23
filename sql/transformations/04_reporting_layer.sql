-- =============================================================================
-- 04_reporting_layer.sql
-- Enterprise BI Reporting System — Core → Reporting Aggregation Layer
--
-- Purpose:
--   Pre-aggregate data into reporting tables consumed by Power BI.
--   Runs after core layer completes. Full refresh on each pipeline run.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. MONTHLY REVENUE TABLE
-- ─────────────────────────────────────────────────────────────────────────────

DELETE FROM rpt_monthly_revenue;

INSERT INTO rpt_monthly_revenue (
    month_label,
    txn_year,
    txn_month,
    total_revenue,
    total_cost,
    total_gross_profit,
    avg_profit_margin,
    transaction_count,
    units_sold
)
SELECT
    txn_month_label                         AS month_label,
    txn_year,
    txn_month,
    ROUND(SUM(revenue), 2)                  AS total_revenue,
    ROUND(SUM(cost), 2)                     AS total_cost,
    ROUND(SUM(gross_profit), 2)             AS total_gross_profit,
    ROUND(AVG(profit_margin), 6)            AS avg_profit_margin,
    COUNT(*)                                AS transaction_count,
    SUM(units_sold)                         AS units_sold
FROM core_sales
GROUP BY txn_month_label, txn_year, txn_month
ORDER BY txn_year, txn_month;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. REGIONAL SUMMARY TABLE
-- ─────────────────────────────────────────────────────────────────────────────

DELETE FROM rpt_regional_summary;

INSERT INTO rpt_regional_summary (
    region,
    txn_year,
    total_revenue,
    total_cost,
    total_gross_profit,
    avg_profit_margin,
    transaction_count
)
SELECT
    region,
    txn_year,
    ROUND(SUM(revenue), 2)          AS total_revenue,
    ROUND(SUM(cost), 2)             AS total_cost,
    ROUND(SUM(gross_profit), 2)     AS total_gross_profit,
    ROUND(AVG(profit_margin), 6)    AS avg_profit_margin,
    COUNT(*)                        AS transaction_count
FROM core_sales
GROUP BY region, txn_year
ORDER BY txn_year, region;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. PRODUCT SUMMARY TABLE
-- ─────────────────────────────────────────────────────────────────────────────

DELETE FROM rpt_product_summary;

INSERT INTO rpt_product_summary (
    product_category,
    product_name,
    txn_year,
    total_revenue,
    total_gross_profit,
    avg_profit_margin,
    units_sold,
    transaction_count
)
SELECT
    product_category,
    product_name,
    txn_year,
    ROUND(SUM(revenue), 2)          AS total_revenue,
    ROUND(SUM(gross_profit), 2)     AS total_gross_profit,
    ROUND(AVG(profit_margin), 6)    AS avg_profit_margin,
    SUM(units_sold)                 AS units_sold,
    COUNT(*)                        AS transaction_count
FROM core_sales
GROUP BY product_category, product_name, txn_year
ORDER BY txn_year, total_revenue DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- REPORTING LAYER VALIDATION
-- ─────────────────────────────────────────────────────────────────────────────

SELECT 'MONTHLY_ROWS'   AS table_name, COUNT(*) AS row_count FROM rpt_monthly_revenue
UNION ALL
SELECT 'REGIONAL_ROWS',  COUNT(*) FROM rpt_regional_summary
UNION ALL
SELECT 'PRODUCT_ROWS',   COUNT(*) FROM rpt_product_summary;

-- Top 5 months by revenue (sanity check)
SELECT
    month_label,
    ROUND(total_revenue, 0)         AS revenue,
    ROUND(total_gross_profit, 0)    AS gross_profit,
    transaction_count
FROM rpt_monthly_revenue
ORDER BY total_revenue DESC
LIMIT 5;

-- Regional performance overview
SELECT
    region,
    txn_year,
    ROUND(total_revenue, 0)         AS revenue,
    ROUND(avg_profit_margin * 100, 1) AS margin_pct
FROM rpt_regional_summary
ORDER BY txn_year, total_revenue DESC;

-- =============================================================================
-- END OF REPORTING LAYER
-- =============================================================================
