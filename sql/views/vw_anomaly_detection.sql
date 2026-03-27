-- =============================================================================
-- vw_anomaly_detection.sql
-- Automated Business Intelligence Reporting System
-- Platform : MS SQL Server 2019+ (T-SQL)
-- Author   : Olayinka Somuyiwa
-- Purpose  : Monthly revenue band chart data with anomaly counts.
--            Consumed by Power BI Page 4 — Anomaly Detection & Ops Metrics.
-- =============================================================================

USE [bi_reporting_db];
GO

CREATE OR ALTER VIEW dbo.vw_anomaly_detection AS
WITH monthly_stats AS (
    SELECT
        txn_month_label,
        txn_year,
        txn_month,
        COUNT(*)                                AS txn_count,
        CAST(SUM(revenue) AS DECIMAL(16,2))     AS total_revenue,
        CAST(AVG(revenue) AS DECIMAL(14,2))     AS avg_revenue,
        CAST(MIN(revenue) AS DECIMAL(14,2))     AS min_revenue,
        CAST(MAX(revenue) AS DECIMAL(14,2))     AS max_revenue,
        -- Standard deviation using T-SQL STDEV aggregate
        CAST(STDEV(revenue) AS DECIMAL(14,2))   AS stddev_revenue
    FROM dbo.core_sales
    GROUP BY txn_month_label, txn_year, txn_month
)
SELECT
    ms.txn_month_label,
    ms.txn_year,
    ms.txn_month,
    ms.txn_count,
    ms.total_revenue,
    ms.avg_revenue,
    ms.min_revenue,
    ms.max_revenue,
    ms.stddev_revenue,

    -- ±2 standard deviation bounds for band chart
    CAST(ms.avg_revenue - 2 * ISNULL(ms.stddev_revenue, 0) AS DECIMAL(14,2)) AS lower_bound_2sd,
    CAST(ms.avg_revenue + 2 * ISNULL(ms.stddev_revenue, 0) AS DECIMAL(14,2)) AS upper_bound_2sd,

    -- Anomaly count per month
    ISNULL((
        SELECT COUNT(*)
        FROM dbo.rpt_anomalies a
        WHERE FORMAT(a.txn_date, 'yyyy-MM') = ms.txn_month_label
    ), 0)                                       AS anomaly_count,

    -- Anomalous revenue total per month
    ISNULL((
        SELECT CAST(SUM(a.revenue) AS DECIMAL(16,2))
        FROM dbo.rpt_anomalies a
        WHERE FORMAT(a.txn_date, 'yyyy-MM') = ms.txn_month_label
    ), 0)                                       AS anomalous_revenue

FROM monthly_stats ms;
GO

-- =============================================================================
-- vw_product_analysis.sql
-- Platform : MS SQL Server 2019+ (T-SQL)
-- Author   : Olayinka Somuyiwa
-- Purpose  : Product and category performance with rankings.
--            Consumed by Power BI Page 3 — Product & Channel Analysis.
-- =============================================================================

CREATE OR ALTER VIEW dbo.vw_product_analysis AS
WITH product_totals AS (
    SELECT
        product_category,
        product_name,
        txn_year,
        SUM(revenue)            AS total_revenue,
        SUM(gross_profit)       AS total_gross_profit,
        AVG(profit_margin)      AS avg_profit_margin,
        SUM(units_sold)         AS units_sold,
        COUNT(*)                AS transaction_count,
        AVG(discount_pct)       AS avg_discount_pct,
        RANK() OVER (
            PARTITION BY product_category, txn_year
            ORDER BY SUM(revenue) DESC
        )                       AS rank_in_category,
        RANK() OVER (
            PARTITION BY txn_year
            ORDER BY SUM(revenue) DESC
        )                       AS overall_rank
    FROM dbo.core_sales
    GROUP BY product_category, product_name, txn_year
)
SELECT
    pt.product_category,
    pt.product_name,
    pt.txn_year,
    CAST(pt.total_revenue AS DECIMAL(16,2))             AS total_revenue,
    CAST(pt.total_gross_profit AS DECIMAL(16,2))        AS total_gross_profit,
    CAST(pt.avg_profit_margin * 100 AS DECIMAL(8,2))    AS profit_margin_pct,
    pt.units_sold,
    pt.transaction_count,
    CAST(pt.avg_discount_pct AS DECIMAL(6,2))           AS avg_discount_pct,
    pt.rank_in_category,
    pt.overall_rank,
    -- Revenue share within category and year
    CAST(
        pt.total_revenue /
        NULLIF(SUM(pt.total_revenue) OVER (PARTITION BY pt.product_category, pt.txn_year), 0)
        * 100 AS DECIMAL(8,2)
    )                                                   AS category_share_pct
FROM product_totals pt;
GO
