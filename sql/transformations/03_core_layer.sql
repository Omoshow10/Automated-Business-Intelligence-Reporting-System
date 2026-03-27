-- =============================================================================
-- 03_core_layer.sql
-- Automated Business Intelligence Reporting System
-- Platform : MS SQL Server 2019+ (T-SQL)
-- Author   : Olayinka Somuyiwa
-- Purpose  : Staging → Core transformation with business logic,
--            date decomposition, and all derived financial metrics.
--            Full refresh (TRUNCATE + INSERT) pattern.
-- =============================================================================

USE [bi_reporting_db];
GO

DECLARE @run_date DATE = CAST(GETDATE() AS DATE);

INSERT INTO dbo.pipeline_log (run_date, stage, status, message)
VALUES (@run_date, 'Transformation', 'Started', 'Core layer transformation initiated');

-- =============================================================================
-- FULL REFRESH: TRUNCATE → INSERT
-- =============================================================================

TRUNCATE TABLE dbo.core_sales;

INSERT INTO dbo.core_sales (
    transaction_id,
    txn_date,
    txn_year,
    txn_quarter,
    txn_month,
    txn_month_label,
    product_name,
    product_category,
    region,
    sales_rep,
    customer_id,
    customer_segment,
    channel,
    units_sold,
    unit_price,
    revenue,
    cost,
    gross_profit,
    profit_margin,
    discount_pct,
    discount_amount,
    is_profitable
)
SELECT
    -- Identity
    s.transaction_id,

    -- Date decomposition (T-SQL functions)
    s.record_date                                           AS txn_date,
    YEAR(s.record_date)                                     AS txn_year,
    DATEPART(QUARTER, s.record_date)                        AS txn_quarter,
    MONTH(s.record_date)                                    AS txn_month,
    FORMAT(s.record_date, 'yyyy-MM')                        AS txn_month_label,

    -- Dimensions (trimmed)
    LTRIM(RTRIM(s.product_name))        AS product_name,
    LTRIM(RTRIM(s.product_category))    AS product_category,
    LTRIM(RTRIM(s.region))              AS region,
    LTRIM(RTRIM(s.sales_rep))           AS sales_rep,
    LTRIM(RTRIM(s.customer_id))         AS customer_id,
    LTRIM(RTRIM(s.customer_segment))    AS customer_segment,
    LTRIM(RTRIM(s.channel))             AS channel,
    s.units_sold,
    s.unit_price,

    -- Financial metrics
    ROUND(s.revenue, 2)                                     AS revenue,
    ROUND(s.cost, 2)                                        AS cost,
    ROUND(s.revenue - s.cost, 2)                            AS gross_profit,

    -- Profit margin ratio (handle divide-by-zero with NULLIF)
    ROUND(
        ISNULL((s.revenue - s.cost) / NULLIF(s.revenue, 0), 0),
        6
    )                                                       AS profit_margin,

    -- Discount
    s.discount_pct,
    ROUND(
        s.unit_price * s.units_sold * (s.discount_pct / 100.0),
        2
    )                                                       AS discount_amount,

    -- Profitability flag
    CASE
        WHEN (s.revenue - s.cost) > 0 THEN 1
        ELSE 0
    END                                                     AS is_profitable

FROM dbo.stg_sales_raw s
WHERE s.transaction_id IS NOT NULL
  AND s.record_date    IS NOT NULL
  AND s.revenue        IS NOT NULL;

-- =============================================================================
-- LOG COMPLETION
-- =============================================================================

DECLARE @core_count INT = (SELECT COUNT(*) FROM dbo.core_sales);

UPDATE dbo.pipeline_log
SET
    status            = 'Completed',
    records_processed = @core_count,
    end_time          = SYSDATETIME(),
    message           = 'Core transformation complete — ' + CAST(@core_count AS NVARCHAR) + ' rows loaded'
WHERE run_date = @run_date AND stage = 'Transformation' AND status = 'Started';

-- =============================================================================
-- CORE LAYER VALIDATION SUMMARY
-- =============================================================================

SELECT
    'CORE_SUMMARY'                              AS layer,
    COUNT(*)                                    AS total_rows,
    COUNT(DISTINCT txn_year)                    AS years_loaded,
    COUNT(DISTINCT region)                      AS regions,
    COUNT(DISTINCT product_category)            AS categories,
    COUNT(DISTINCT sales_rep)                   AS sales_reps,
    CAST(SUM(revenue) AS DECIMAL(16,2))         AS total_revenue,
    CAST(SUM(gross_profit) AS DECIMAL(16,2))    AS total_gross_profit,
    CAST(AVG(profit_margin) * 100 AS DECIMAL(8,2)) AS avg_profit_margin_pct,
    SUM(CASE WHEN is_profitable = 0 THEN 1 ELSE 0 END) AS unprofitable_txns
FROM dbo.core_sales;

-- Year-over-year breakdown
SELECT
    txn_year,
    COUNT(*)                                        AS transactions,
    CAST(SUM(revenue) AS DECIMAL(16,2))             AS total_revenue,
    CAST(SUM(gross_profit) AS DECIMAL(16,2))        AS total_gross_profit,
    CAST(AVG(profit_margin) * 100 AS DECIMAL(8,2))  AS avg_margin_pct
FROM dbo.core_sales
GROUP BY txn_year
ORDER BY txn_year;
GO

-- =============================================================================
-- END OF CORE LAYER
-- =============================================================================
