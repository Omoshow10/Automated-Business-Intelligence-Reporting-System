-- =============================================================================
-- vw_regional_performance.sql
-- Revenue, profit and growth by region — Power BI Page 2
-- =============================================================================

DROP VIEW IF EXISTS vw_regional_performance;

CREATE VIEW vw_regional_performance AS
WITH regional_yearly AS (
    SELECT
        region,
        txn_year,
        total_revenue,
        total_cost,
        total_gross_profit,
        avg_profit_margin,
        transaction_count
    FROM rpt_regional_summary
),
with_yoy AS (
    SELECT
        r.*,
        py.total_revenue    AS prior_year_revenue,
        py.total_gross_profit AS prior_year_profit
    FROM regional_yearly r
    LEFT JOIN regional_yearly py
        ON py.region   = r.region
       AND py.txn_year = r.txn_year - 1
),
channel_mix AS (
    SELECT
        region,
        txn_year,
        channel,
        ROUND(SUM(revenue), 2)  AS channel_revenue,
        COUNT(*)                AS channel_txns
    FROM core_sales
    GROUP BY region, txn_year, channel
),
top_reps AS (
    SELECT
        region,
        txn_year,
        sales_rep,
        ROUND(SUM(revenue), 2)      AS rep_revenue,
        RANK() OVER (
            PARTITION BY region, txn_year
            ORDER BY SUM(revenue) DESC
        )                           AS rep_rank
    FROM core_sales
    GROUP BY region, txn_year, sales_rep
)
SELECT
    r.region,
    r.txn_year,
    ROUND(r.total_revenue, 2)               AS total_revenue,
    ROUND(r.total_cost, 2)                  AS total_cost,
    ROUND(r.total_gross_profit, 2)          AS total_gross_profit,
    ROUND(r.avg_profit_margin * 100, 2)     AS profit_margin_pct,
    r.transaction_count,

    -- YoY growth
    CASE
        WHEN r.prior_year_revenue IS NULL OR r.prior_year_revenue = 0 THEN NULL
        ELSE ROUND(
            (r.total_revenue - r.prior_year_revenue) / r.prior_year_revenue * 100, 2
        )
    END                                     AS yoy_revenue_growth_pct,

    -- Revenue share across all regions (this year)
    ROUND(
        r.total_revenue / SUM(r.total_revenue) OVER (PARTITION BY r.txn_year) * 100, 2
    )                                       AS region_revenue_share_pct,

    -- Avg transaction size
    ROUND(r.total_revenue / NULLIF(r.transaction_count, 0), 2) AS avg_transaction_value,

    -- Top sales rep in this region/year
    (SELECT tr.sales_rep FROM top_reps tr
     WHERE tr.region = r.region AND tr.txn_year = r.txn_year AND tr.rep_rank = 1
     LIMIT 1)                               AS top_sales_rep,

    (SELECT tr.rep_revenue FROM top_reps tr
     WHERE tr.region = r.region AND tr.txn_year = r.txn_year AND tr.rep_rank = 1
     LIMIT 1)                               AS top_rep_revenue

FROM with_yoy r
ORDER BY r.txn_year, r.total_revenue DESC;
