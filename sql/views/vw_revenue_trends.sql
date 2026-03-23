-- =============================================================================
-- vw_revenue_trends.sql
-- Monthly revenue with MoM and YoY growth comparisons
-- Power BI Page 1 — Executive Summary (trend lines)
-- =============================================================================

DROP VIEW IF EXISTS vw_revenue_trends;

CREATE VIEW vw_revenue_trends AS
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
        units_sold,
        -- Row number for lag calculations
        ROW_NUMBER() OVER (ORDER BY txn_year, txn_month) AS rn
    FROM rpt_monthly_revenue
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
        -- Previous month revenue (MoM)
        prev_m.total_revenue                AS prev_month_revenue,
        -- Same month prior year (YoY)
        prev_y.total_revenue                AS prev_year_revenue
    FROM monthly m
    LEFT JOIN monthly prev_m
        ON prev_m.rn = m.rn - 1
    LEFT JOIN monthly prev_y
        ON prev_y.txn_year  = m.txn_year - 1
       AND prev_y.txn_month = m.txn_month
)
SELECT
    month_label,
    txn_year,
    txn_month,
    ROUND(total_revenue, 2)             AS total_revenue,
    ROUND(total_cost, 2)                AS total_cost,
    ROUND(total_gross_profit, 2)        AS total_gross_profit,
    ROUND(avg_profit_margin * 100, 2)   AS profit_margin_pct,
    transaction_count,
    units_sold,

    -- Month-over-Month growth
    CASE
        WHEN prev_month_revenue IS NULL OR prev_month_revenue = 0 THEN NULL
        ELSE ROUND((total_revenue - prev_month_revenue) / prev_month_revenue * 100, 2)
    END                                 AS mom_growth_pct,

    -- Year-over-Year growth
    CASE
        WHEN prev_year_revenue IS NULL OR prev_year_revenue = 0 THEN NULL
        ELSE ROUND((total_revenue - prev_year_revenue) / prev_year_revenue * 100, 2)
    END                                 AS yoy_growth_pct,

    -- Running total for waterfall charts
    SUM(total_revenue) OVER (
        PARTITION BY txn_year
        ORDER BY txn_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                   AS ytd_revenue

FROM with_lag
ORDER BY txn_year, txn_month;

-- Quick preview
SELECT * FROM vw_revenue_trends ORDER BY month_label LIMIT 10;
