-- =============================================================================
-- vw_revenue_trends.sql
-- Automated Business Intelligence Reporting System
-- Platform : MS SQL Server 2019+ (T-SQL)
-- Author   : Olayinka Somuyiwa
-- Purpose  : Monthly revenue with MoM and YoY growth comparisons.
--            Consumed by Power BI Page 1 — Executive Summary.
-- =============================================================================

USE [bi_reporting_db];
GO

CREATE OR ALTER VIEW dbo.vw_revenue_trends AS
WITH monthly AS (
    SELECT
        month_label,
        txn_year,
        txn_month,
        total_revenue,
        total_cost,
        total_gross_profit,
        avg_profit_margin,
        transaction_count,
        units_sold
    FROM dbo.rpt_monthly_revenue
),
with_lag AS (
    SELECT
        m.month_label,
        m.txn_year,
        m.txn_month,
        m.total_revenue,
        m.total_cost,
        m.total_gross_profit,
        m.avg_profit_margin,
        m.transaction_count,
        m.units_sold,
        -- Previous month (MoM) using LAG window function
        LAG(m.total_revenue, 1) OVER (ORDER BY m.txn_year, m.txn_month)    AS prev_month_revenue,
        -- Same month prior year (YoY) via self-join
        py.total_revenue                                                     AS prev_year_revenue
    FROM monthly m
    LEFT JOIN monthly py
        ON py.txn_year  = m.txn_year - 1
       AND py.txn_month = m.txn_month
)
SELECT
    month_label,
    txn_year,
    txn_month,
    CAST(total_revenue AS DECIMAL(16,2))            AS total_revenue,
    CAST(total_cost AS DECIMAL(16,2))               AS total_cost,
    CAST(total_gross_profit AS DECIMAL(16,2))       AS total_gross_profit,
    CAST(avg_profit_margin * 100 AS DECIMAL(8,2))   AS profit_margin_pct,
    transaction_count,
    units_sold,

    -- Month-over-Month growth %
    CASE
        WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
        ELSE CAST(
            (total_revenue - prev_month_revenue) / prev_month_revenue * 100
            AS DECIMAL(8,2)
        )
    END                                             AS mom_growth_pct,

    -- Year-over-Year growth %
    CASE
        WHEN prev_year_revenue IS NULL OR prev_year_revenue = 0 THEN NULL
        ELSE CAST(
            (total_revenue - prev_year_revenue) / prev_year_revenue * 100
            AS DECIMAL(8,2)
        )
    END                                             AS yoy_growth_pct,

    -- Year-to-date running total (resets each January)
    SUM(total_revenue) OVER (
        PARTITION BY txn_year
        ORDER BY txn_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                               AS ytd_revenue

FROM with_lag;
GO

-- Quick preview
SELECT TOP 12 * FROM dbo.vw_revenue_trends ORDER BY txn_year, txn_month;
GO
