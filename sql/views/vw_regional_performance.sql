-- =============================================================================
-- vw_regional_performance.sql
-- Automated Business Intelligence Reporting System
-- Platform : MS SQL Server 2019+ (T-SQL)
-- Author   : Olayinka Somuyiwa
-- Purpose  : Revenue, profit, and YoY growth by region.
--            Consumed by Power BI Page 2 — Regional Performance.
-- =============================================================================

USE [bi_reporting_db];
GO

CREATE OR ALTER VIEW dbo.vw_regional_performance AS
WITH regional_yoy AS (
    SELECT
        r.region,
        r.txn_year,
        r.total_revenue,
        r.total_cost,
        r.total_gross_profit,
        r.avg_profit_margin,
        r.transaction_count,
        py.total_revenue    AS prior_year_revenue,
        py.total_gross_profit AS prior_year_profit
    FROM dbo.rpt_regional_summary r
    LEFT JOIN dbo.rpt_regional_summary py
        ON py.region   = r.region
       AND py.txn_year = r.txn_year - 1
),
top_reps AS (
    SELECT
        region,
        txn_year,
        sales_rep,
        SUM(revenue)    AS rep_revenue,
        RANK() OVER (
            PARTITION BY region, txn_year
            ORDER BY SUM(revenue) DESC
        )               AS rep_rank
    FROM dbo.core_sales
    GROUP BY region, txn_year, sales_rep
)
SELECT
    r.region,
    r.txn_year,
    CAST(r.total_revenue AS DECIMAL(16,2))              AS total_revenue,
    CAST(r.total_cost AS DECIMAL(16,2))                 AS total_cost,
    CAST(r.total_gross_profit AS DECIMAL(16,2))         AS total_gross_profit,
    CAST(r.avg_profit_margin * 100 AS DECIMAL(8,2))     AS profit_margin_pct,
    r.transaction_count,

    -- YoY revenue growth
    CASE
        WHEN r.prior_year_revenue IS NULL OR r.prior_year_revenue = 0 THEN NULL
        ELSE CAST(
            (r.total_revenue - r.prior_year_revenue) / r.prior_year_revenue * 100
            AS DECIMAL(8,2)
        )
    END                                                 AS yoy_revenue_growth_pct,

    -- Region revenue share within year
    CAST(
        r.total_revenue /
        NULLIF(SUM(r.total_revenue) OVER (PARTITION BY r.txn_year), 0) * 100
        AS DECIMAL(8,2)
    )                                                   AS region_revenue_share_pct,

    -- Average transaction value
    CAST(
        r.total_revenue / NULLIF(r.transaction_count, 0)
        AS DECIMAL(14,2)
    )                                                   AS avg_transaction_value,

    -- Top rep in this region/year
    tr.sales_rep                                        AS top_sales_rep,
    CAST(tr.rep_revenue AS DECIMAL(14,2))               AS top_rep_revenue

FROM regional_yoy r
LEFT JOIN top_reps tr
    ON tr.region   = r.region
   AND tr.txn_year = r.txn_year
   AND tr.rep_rank = 1;
GO
