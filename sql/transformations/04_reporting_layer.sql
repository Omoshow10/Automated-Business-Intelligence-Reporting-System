-- =============================================================================
-- 04_reporting_layer.sql
-- Automated Business Intelligence Reporting System
-- Platform : MS SQL Server 2019+ (T-SQL)
-- Author   : Olayinka Somuyiwa
-- Purpose  : Core → Reporting layer aggregation.
--            Pre-aggregated tables consumed directly by Power BI.
--            Full refresh on each pipeline run.
-- =============================================================================

USE [bi_reporting_db];
GO

DECLARE @run_date DATE = CAST(GETDATE() AS DATE);

INSERT INTO dbo.pipeline_log (run_date, stage, status, message)
VALUES (@run_date, 'Reporting', 'Started', 'Reporting layer aggregation initiated');

-- =============================================================================
-- 1. MONTHLY REVENUE SUMMARY
-- =============================================================================

TRUNCATE TABLE dbo.rpt_monthly_revenue;

INSERT INTO dbo.rpt_monthly_revenue (
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
FROM dbo.core_sales
GROUP BY txn_month_label, txn_year, txn_month;

-- =============================================================================
-- 2. REGIONAL SUMMARY
-- =============================================================================

TRUNCATE TABLE dbo.rpt_regional_summary;

INSERT INTO dbo.rpt_regional_summary (
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
FROM dbo.core_sales
GROUP BY region, txn_year;

-- =============================================================================
-- 3. PRODUCT SUMMARY
-- =============================================================================

TRUNCATE TABLE dbo.rpt_product_summary;

INSERT INTO dbo.rpt_product_summary (
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
FROM dbo.core_sales
GROUP BY product_category, product_name, txn_year;

-- =============================================================================
-- LOG COMPLETION
-- =============================================================================

UPDATE dbo.pipeline_log
SET
    status            = 'Completed',
    records_processed = (SELECT COUNT(*) FROM dbo.rpt_monthly_revenue)
                      + (SELECT COUNT(*) FROM dbo.rpt_regional_summary)
                      + (SELECT COUNT(*) FROM dbo.rpt_product_summary),
    end_time          = SYSDATETIME(),
    message           = 'Reporting layer aggregation complete'
WHERE run_date = @run_date AND stage = 'Reporting' AND status = 'Started';

-- =============================================================================
-- VALIDATION
-- =============================================================================

SELECT 'rpt_monthly_revenue'  AS table_name, COUNT(*) AS row_count FROM dbo.rpt_monthly_revenue
UNION ALL
SELECT 'rpt_regional_summary', COUNT(*) FROM dbo.rpt_regional_summary
UNION ALL
SELECT 'rpt_product_summary',  COUNT(*) FROM dbo.rpt_product_summary;

-- Top 5 months by revenue
SELECT TOP 5
    month_label,
    CAST(total_revenue AS DECIMAL(16,0))        AS revenue,
    CAST(total_gross_profit AS DECIMAL(16,0))   AS gross_profit,
    transaction_count
FROM dbo.rpt_monthly_revenue
ORDER BY total_revenue DESC;

-- Regional performance overview
SELECT
    region,
    txn_year,
    CAST(total_revenue AS DECIMAL(16,0))            AS revenue,
    CAST(avg_profit_margin * 100 AS DECIMAL(8,1))   AS margin_pct
FROM dbo.rpt_regional_summary
ORDER BY txn_year, total_revenue DESC;
GO

-- =============================================================================
-- END OF REPORTING LAYER
-- =============================================================================
