-- =============================================================================
-- 03_core_layer.sql
-- Enterprise BI Reporting System — Staging → Core Transformation
--
-- Purpose:
--   Apply business logic, derive calculated fields, cast types.
--   This is the single source of truth for all downstream reporting.
--   Follows SCD Type 1 (full refresh) pattern.
-- =============================================================================

-- Full refresh of core layer
DELETE FROM core_sales;

-- -----------------------------------------------------------------------------
-- INSERT: Staging → Core with full enrichment
-- -----------------------------------------------------------------------------

INSERT INTO core_sales (
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

    -- Date decomposition (SQLite date functions)
    s.date                                                      AS txn_date,
    CAST(strftime('%Y', s.date) AS INTEGER)                     AS txn_year,
    CAST((strftime('%m', s.date) - 1) / 3 + 1 AS INTEGER)      AS txn_quarter,
    CAST(strftime('%m', s.date) AS INTEGER)                     AS txn_month,
    strftime('%Y-%m', s.date)                                   AS txn_month_label,

    -- Dimensions
    TRIM(s.product_name)        AS product_name,
    TRIM(s.product_category)    AS product_category,
    TRIM(s.region)              AS region,
    TRIM(s.sales_rep)           AS sales_rep,
    TRIM(s.customer_id)         AS customer_id,
    TRIM(s.customer_segment)    AS customer_segment,
    TRIM(s.channel)             AS channel,
    s.units_sold,
    s.unit_price,

    -- Financial metrics
    ROUND(s.revenue, 2)                                         AS revenue,
    ROUND(s.cost, 2)                                            AS cost,
    ROUND(s.revenue - s.cost, 2)                                AS gross_profit,

    -- Profit margin: handle divide-by-zero
    CASE
        WHEN s.revenue = 0 THEN 0
        ELSE ROUND((s.revenue - s.cost) / s.revenue, 6)
    END                                                         AS profit_margin,

    -- Discount
    s.discount_pct,
    ROUND(s.unit_price * s.units_sold * (s.discount_pct / 100.0), 2) AS discount_amount,

    -- Profitability flag
    CASE
        WHEN (s.revenue - s.cost) > 0 THEN 1
        ELSE 0
    END                                                         AS is_profitable

FROM stg_sales_raw s
WHERE s.transaction_id IS NOT NULL
  AND s.date IS NOT NULL
  AND s.revenue IS NOT NULL;

-- -----------------------------------------------------------------------------
-- CORE LAYER SUMMARY — validation check
-- -----------------------------------------------------------------------------

SELECT
    'CORE_SUMMARY'                  AS layer,
    COUNT(*)                        AS total_rows,
    COUNT(DISTINCT txn_year)        AS years_loaded,
    COUNT(DISTINCT region)          AS regions,
    COUNT(DISTINCT product_category) AS categories,
    COUNT(DISTINCT sales_rep)       AS sales_reps,
    ROUND(SUM(revenue), 2)          AS total_revenue,
    ROUND(SUM(gross_profit), 2)     AS total_gross_profit,
    ROUND(AVG(profit_margin) * 100, 2) AS avg_profit_margin_pct,
    SUM(CASE WHEN is_profitable = 0 THEN 1 ELSE 0 END) AS unprofitable_txns
FROM core_sales;

-- -----------------------------------------------------------------------------
-- YEAR-OVER-YEAR GROWTH VALIDATION
-- -----------------------------------------------------------------------------

SELECT
    txn_year,
    COUNT(*)                            AS transactions,
    ROUND(SUM(revenue), 2)              AS total_revenue,
    ROUND(SUM(gross_profit), 2)         AS total_gross_profit,
    ROUND(AVG(profit_margin) * 100, 2)  AS avg_margin_pct
FROM core_sales
GROUP BY txn_year
ORDER BY txn_year;

-- =============================================================================
-- END OF CORE LAYER
-- =============================================================================
