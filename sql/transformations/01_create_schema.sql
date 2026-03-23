-- =============================================================================
-- 01_create_schema.sql
-- Enterprise BI Reporting System — Database & Table Setup
-- Compatible with: SQLite 3.x | SQL Server 2019+ | PostgreSQL 13+
-- =============================================================================

-- -----------------------------------------------------------------------------
-- DROP EXISTING OBJECTS (safe re-run)
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS stg_sales_raw;
DROP TABLE IF EXISTS core_sales;
DROP TABLE IF EXISTS rpt_monthly_revenue;
DROP TABLE IF EXISTS rpt_regional_summary;
DROP TABLE IF EXISTS rpt_product_summary;
DROP TABLE IF EXISTS rpt_anomalies;

DROP VIEW IF EXISTS vw_revenue_trends;
DROP VIEW IF EXISTS vw_regional_performance;
DROP VIEW IF EXISTS vw_product_analysis;
DROP VIEW IF EXISTS vw_anomaly_detection;

-- -----------------------------------------------------------------------------
-- STAGING LAYER — mirrors raw CSV structure, minimal typing
-- -----------------------------------------------------------------------------

CREATE TABLE stg_sales_raw (
    transaction_id      TEXT            NOT NULL,
    date                TEXT            NOT NULL,           -- stored as ISO string
    product_name        TEXT            NOT NULL,
    product_category    TEXT            NOT NULL,
    region              TEXT            NOT NULL,
    sales_rep           TEXT            NOT NULL,
    customer_id         TEXT            NOT NULL,
    customer_segment    TEXT            NOT NULL,
    channel             TEXT            NOT NULL,
    units_sold          INTEGER         NOT NULL DEFAULT 0,
    unit_price          REAL            NOT NULL DEFAULT 0,
    revenue             REAL            NOT NULL DEFAULT 0,
    cost                REAL            NOT NULL DEFAULT 0,
    discount_pct        REAL            NOT NULL DEFAULT 0, -- as percentage (0–40)
    loaded_at           TEXT            NOT NULL DEFAULT (datetime('now'))
);

-- -----------------------------------------------------------------------------
-- CORE LAYER — typed, validated, enriched with derived metrics
-- -----------------------------------------------------------------------------

CREATE TABLE core_sales (
    transaction_id      TEXT            NOT NULL PRIMARY KEY,
    txn_date            TEXT            NOT NULL,           -- YYYY-MM-DD
    txn_year            INTEGER         NOT NULL,
    txn_quarter         INTEGER         NOT NULL,           -- 1–4
    txn_month           INTEGER         NOT NULL,           -- 1–12
    txn_month_label     TEXT            NOT NULL,           -- 2024-03
    product_name        TEXT            NOT NULL,
    product_category    TEXT            NOT NULL,
    region              TEXT            NOT NULL,
    sales_rep           TEXT            NOT NULL,
    customer_id         TEXT            NOT NULL,
    customer_segment    TEXT            NOT NULL,
    channel             TEXT            NOT NULL,
    units_sold          INTEGER         NOT NULL,
    unit_price          REAL            NOT NULL,
    revenue             REAL            NOT NULL,
    cost                REAL            NOT NULL,
    gross_profit        REAL            NOT NULL,           -- revenue - cost
    profit_margin       REAL            NOT NULL,           -- gross_profit / revenue
    discount_pct        REAL            NOT NULL,
    discount_amount     REAL            NOT NULL,           -- unit_price * units * (discount_pct/100)
    is_profitable       INTEGER         NOT NULL,           -- 1 if profit_margin > 0, else 0
    transformed_at      TEXT            NOT NULL DEFAULT (datetime('now'))
);

-- -----------------------------------------------------------------------------
-- REPORTING LAYER — pre-aggregated for dashboard performance
-- -----------------------------------------------------------------------------

CREATE TABLE rpt_monthly_revenue (
    month_label         TEXT            NOT NULL PRIMARY KEY, -- 2024-03
    txn_year            INTEGER         NOT NULL,
    txn_month           INTEGER         NOT NULL,
    total_revenue       REAL            NOT NULL DEFAULT 0,
    total_cost          REAL            NOT NULL DEFAULT 0,
    total_gross_profit  REAL            NOT NULL DEFAULT 0,
    avg_profit_margin   REAL            NOT NULL DEFAULT 0,
    transaction_count   INTEGER         NOT NULL DEFAULT 0,
    units_sold          INTEGER         NOT NULL DEFAULT 0
);

CREATE TABLE rpt_regional_summary (
    region              TEXT            NOT NULL,
    txn_year            INTEGER         NOT NULL,
    total_revenue       REAL            NOT NULL DEFAULT 0,
    total_cost          REAL            NOT NULL DEFAULT 0,
    total_gross_profit  REAL            NOT NULL DEFAULT 0,
    avg_profit_margin   REAL            NOT NULL DEFAULT 0,
    transaction_count   INTEGER         NOT NULL DEFAULT 0,
    PRIMARY KEY (region, txn_year)
);

CREATE TABLE rpt_product_summary (
    product_category    TEXT            NOT NULL,
    product_name        TEXT            NOT NULL,
    txn_year            INTEGER         NOT NULL,
    total_revenue       REAL            NOT NULL DEFAULT 0,
    total_gross_profit  REAL            NOT NULL DEFAULT 0,
    avg_profit_margin   REAL            NOT NULL DEFAULT 0,
    units_sold          INTEGER         NOT NULL DEFAULT 0,
    transaction_count   INTEGER         NOT NULL DEFAULT 0,
    PRIMARY KEY (product_name, txn_year)
);

CREATE TABLE rpt_anomalies (
    transaction_id      TEXT            NOT NULL PRIMARY KEY,
    txn_date            TEXT            NOT NULL,
    product_name        TEXT            NOT NULL,
    region              TEXT            NOT NULL,
    revenue             REAL            NOT NULL,
    cost                REAL            NOT NULL,
    revenue_zscore      REAL,
    cost_zscore         REAL,
    anomaly_type        TEXT,           -- 'HIGH_REVENUE' | 'LOW_REVENUE' | 'HIGH_COST' | 'DISCOUNT_OUTLIER'
    detected_at         TEXT            NOT NULL DEFAULT (datetime('now'))
);

-- -----------------------------------------------------------------------------
-- INDEXES for dashboard query performance
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_core_date       ON core_sales (txn_date);
CREATE INDEX IF NOT EXISTS idx_core_year_month ON core_sales (txn_year, txn_month);
CREATE INDEX IF NOT EXISTS idx_core_region     ON core_sales (region);
CREATE INDEX IF NOT EXISTS idx_core_category   ON core_sales (product_category);
CREATE INDEX IF NOT EXISTS idx_core_channel    ON core_sales (channel);
CREATE INDEX IF NOT EXISTS idx_core_segment    ON core_sales (customer_segment);
CREATE INDEX IF NOT EXISTS idx_core_rep        ON core_sales (sales_rep);

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
