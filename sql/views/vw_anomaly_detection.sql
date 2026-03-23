-- =============================================================================
-- vw_anomaly_detection.sql
-- Statistical anomaly detection using Z-score approach in SQL
-- Power BI Page 4 — Anomaly Detection & Ops Metrics
--
-- Note: Full Z-score computation done in Python (anomaly_detector.py).
-- This view surfaces the pre-computed anomaly results from rpt_anomalies.
-- It also provides the statistical baseline for the Power BI band chart.
-- =============================================================================

DROP VIEW IF EXISTS vw_anomaly_detection;

CREATE VIEW vw_anomaly_detection AS
WITH monthly_stats AS (
    -- Compute per-month revenue statistics for band chart
    SELECT
        txn_month_label,
        txn_year,
        txn_month,
        COUNT(*)                        AS txn_count,
        ROUND(SUM(revenue), 2)          AS total_revenue,
        ROUND(AVG(revenue), 2)          AS avg_revenue,
        ROUND(MIN(revenue), 2)          AS min_revenue,
        ROUND(MAX(revenue), 2)          AS max_revenue,
        -- Approximate std dev using variance formula
        ROUND(
            SQRT(
                SUM(revenue * revenue) / COUNT(*) -
                (SUM(revenue) / COUNT(*)) * (SUM(revenue) / COUNT(*))
            ), 2
        )                               AS stddev_revenue
    FROM core_sales
    GROUP BY txn_month_label, txn_year, txn_month
),
anomaly_summary AS (
    SELECT
        txn_date,
        COUNT(*)                        AS anomaly_count,
        SUM(revenue)                    AS anomalous_revenue,
        GROUP_CONCAT(anomaly_type, ', ') AS anomaly_types
    FROM rpt_anomalies
    GROUP BY txn_date
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

    -- Expected revenue band (±2 std dev)
    ROUND(ms.avg_revenue - 2 * ms.stddev_revenue, 2)   AS lower_bound_2sd,
    ROUND(ms.avg_revenue + 2 * ms.stddev_revenue, 2)   AS upper_bound_2sd,

    -- Count anomalous transactions in this month
    COALESCE((
        SELECT COUNT(*)
        FROM rpt_anomalies a
        WHERE strftime('%Y-%m', a.txn_date) = ms.txn_month_label
    ), 0)                                               AS anomaly_count,

    COALESCE((
        SELECT ROUND(SUM(a.revenue), 2)
        FROM rpt_anomalies a
        WHERE strftime('%Y-%m', a.txn_date) = ms.txn_month_label
    ), 0)                                               AS anomalous_revenue

FROM monthly_stats ms
ORDER BY ms.txn_year, ms.txn_month;


-- =============================================================================
-- vw_product_analysis.sql — Product & Channel deep-dive
-- Power BI Page 3
-- =============================================================================

DROP VIEW IF EXISTS vw_product_analysis;

CREATE VIEW vw_product_analysis AS
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
        -- Rank within category by revenue
        RANK() OVER (
            PARTITION BY product_category, txn_year
            ORDER BY SUM(revenue) DESC
        )                       AS rank_in_category,
        -- Rank overall by revenue
        RANK() OVER (
            PARTITION BY txn_year
            ORDER BY SUM(revenue) DESC
        )                       AS overall_rank
    FROM core_sales
    GROUP BY product_category, product_name, txn_year
),
channel_revenue AS (
    SELECT
        channel,
        txn_year,
        ROUND(SUM(revenue), 2)      AS channel_revenue,
        ROUND(AVG(profit_margin) * 100, 2) AS channel_margin_pct,
        COUNT(*)                    AS transactions,
        COUNT(DISTINCT customer_id) AS unique_customers
    FROM core_sales
    GROUP BY channel, txn_year
),
segment_revenue AS (
    SELECT
        customer_segment,
        txn_year,
        ROUND(SUM(revenue), 2)              AS segment_revenue,
        ROUND(AVG(profit_margin) * 100, 2)  AS segment_margin_pct,
        COUNT(DISTINCT customer_id)         AS unique_customers,
        ROUND(AVG(discount_pct), 2)         AS avg_discount_pct
    FROM core_sales
    GROUP BY customer_segment, txn_year
)
-- Product view (primary)
SELECT
    'PRODUCT'                           AS view_type,
    pt.product_category,
    pt.product_name,
    pt.txn_year,
    ROUND(pt.total_revenue, 2)          AS total_revenue,
    ROUND(pt.total_gross_profit, 2)     AS total_gross_profit,
    ROUND(pt.avg_profit_margin * 100, 2) AS profit_margin_pct,
    pt.units_sold,
    pt.transaction_count,
    ROUND(pt.avg_discount_pct, 2)       AS avg_discount_pct,
    pt.rank_in_category,
    pt.overall_rank,
    -- Revenue share within category
    ROUND(
        pt.total_revenue /
        SUM(pt.total_revenue) OVER (PARTITION BY pt.product_category, pt.txn_year) * 100, 2
    )                                   AS category_share_pct,
    NULL AS channel,
    NULL AS customer_segment

FROM product_totals pt

ORDER BY pt.txn_year, pt.overall_rank;
