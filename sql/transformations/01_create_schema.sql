-- =============================================================================
-- 01_create_schema.sql
-- Automated Business Intelligence Reporting System
-- Platform  : MS SQL Server 2019+ (T-SQL)
-- Author    : Olayinka Somuyiwa
-- Purpose   : Create all database objects — tables, indexes, and audit log
-- Run once  : Execute in SSMS against [bi_reporting_db]
-- =============================================================================

USE [bi_reporting_db];
GO

-- =============================================================================
-- DROP EXISTING OBJECTS (safe re-run)
-- =============================================================================

IF OBJECT_ID('dbo.rpt_anomalies',            'U') IS NOT NULL DROP TABLE dbo.rpt_anomalies;
IF OBJECT_ID('dbo.rpt_product_summary',      'U') IS NOT NULL DROP TABLE dbo.rpt_product_summary;
IF OBJECT_ID('dbo.rpt_regional_summary',     'U') IS NOT NULL DROP TABLE dbo.rpt_regional_summary;
IF OBJECT_ID('dbo.rpt_monthly_revenue',      'U') IS NOT NULL DROP TABLE dbo.rpt_monthly_revenue;
IF OBJECT_ID('dbo.reporting_summary',        'U') IS NOT NULL DROP TABLE dbo.reporting_summary;
IF OBJECT_ID('dbo.core_sales',               'U') IS NOT NULL DROP TABLE dbo.core_sales;
IF OBJECT_ID('dbo.staging_operational_data', 'U') IS NOT NULL DROP TABLE dbo.staging_operational_data;
IF OBJECT_ID('dbo.stg_sales_raw',            'U') IS NOT NULL DROP TABLE dbo.stg_sales_raw;
IF OBJECT_ID('dbo.pipeline_log',             'U') IS NOT NULL DROP TABLE dbo.pipeline_log;
GO

-- =============================================================================
-- PIPELINE AUDIT / LOG TABLE
-- Records every stage execution for monitoring and auditability
-- =============================================================================

CREATE TABLE dbo.pipeline_log (
    log_id              INT             IDENTITY(1,1)   NOT NULL,
    run_date            DATE            NOT NULL,
    stage               NVARCHAR(50)    NOT NULL,   -- 'Extraction' | 'Transformation' | 'Reporting'
    status              NVARCHAR(20)    NOT NULL,   -- 'Started' | 'Completed' | 'Failed'
    message             NVARCHAR(500)   NULL,
    records_processed   INT             NULL,
    start_time          DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    end_time            DATETIME2       NULL,
    duration_seconds    AS              DATEDIFF(SECOND, start_time, ISNULL(end_time, SYSDATETIME())),
    error_message       NVARCHAR(MAX)   NULL,
    CONSTRAINT PK_pipeline_log PRIMARY KEY CLUSTERED (log_id)
);
GO

-- =============================================================================
-- STAGING LAYER
-- Mirrors raw operational source — minimal typing, full row preservation
-- =============================================================================

CREATE TABLE dbo.stg_sales_raw (
    transaction_id      NVARCHAR(20)    NOT NULL,
    record_date         DATE            NOT NULL,
    product_name        NVARCHAR(100)   NOT NULL,
    product_category    NVARCHAR(50)    NOT NULL,
    region              NVARCHAR(50)    NOT NULL,
    sales_rep           NVARCHAR(100)   NOT NULL,
    customer_id         NVARCHAR(20)    NOT NULL,
    customer_segment    NVARCHAR(30)    NOT NULL,
    channel             NVARCHAR(30)    NOT NULL,
    units_sold          INT             NOT NULL DEFAULT 0,
    unit_price          DECIMAL(12,2)   NOT NULL DEFAULT 0,
    revenue             DECIMAL(14,2)   NOT NULL DEFAULT 0,
    cost                DECIMAL(14,2)   NOT NULL DEFAULT 0,
    discount_pct        DECIMAL(6,2)    NOT NULL DEFAULT 0,
    source_system       NVARCHAR(50)    NULL     DEFAULT 'CSV_LOAD',
    loaded_at           DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_stg_sales_raw PRIMARY KEY NONCLUSTERED (transaction_id)
);
GO

-- Generic operational staging (consumed by usp_DailyDataExtraction)
CREATE TABLE dbo.staging_operational_data (
    staging_id          INT             IDENTITY(1,1)   NOT NULL,
    record_date         DATE            NOT NULL,
    entity_id           NVARCHAR(50)    NOT NULL,
    metric_name         NVARCHAR(100)   NOT NULL,
    metric_value        DECIMAL(18,4)   NOT NULL,
    source_system       NVARCHAR(50)    NOT NULL,
    loaded_at           DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_staging_operational_data PRIMARY KEY CLUSTERED (staging_id)
);

CREATE NONCLUSTERED INDEX IX_staging_record_date
    ON dbo.staging_operational_data (record_date);
GO

-- =============================================================================
-- CORE LAYER
-- Typed, validated, enriched with all derived business metrics
-- =============================================================================

CREATE TABLE dbo.core_sales (
    transaction_id      NVARCHAR(20)    NOT NULL,
    txn_date            DATE            NOT NULL,
    txn_year            SMALLINT        NOT NULL,
    txn_quarter         TINYINT         NOT NULL,
    txn_month           TINYINT         NOT NULL,
    txn_month_label     NVARCHAR(7)     NOT NULL,   -- YYYY-MM
    product_name        NVARCHAR(100)   NOT NULL,
    product_category    NVARCHAR(50)    NOT NULL,
    region              NVARCHAR(50)    NOT NULL,
    sales_rep           NVARCHAR(100)   NOT NULL,
    customer_id         NVARCHAR(20)    NOT NULL,
    customer_segment    NVARCHAR(30)    NOT NULL,
    channel             NVARCHAR(30)    NOT NULL,
    units_sold          INT             NOT NULL,
    unit_price          DECIMAL(12,2)   NOT NULL,
    revenue             DECIMAL(14,2)   NOT NULL,
    cost                DECIMAL(14,2)   NOT NULL,
    gross_profit        DECIMAL(14,2)   NOT NULL,   -- revenue - cost
    profit_margin       DECIMAL(8,6)    NOT NULL,   -- ratio: gross_profit / revenue
    discount_pct        DECIMAL(6,2)    NOT NULL,
    discount_amount     DECIMAL(14,2)   NOT NULL,
    is_profitable       BIT             NOT NULL DEFAULT 1,
    transformed_at      DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_core_sales PRIMARY KEY CLUSTERED (transaction_id)
);
GO

-- =============================================================================
-- REPORTING LAYER — Pre-aggregated tables for Power BI consumption
-- =============================================================================

CREATE TABLE dbo.rpt_monthly_revenue (
    month_label         NVARCHAR(7)     NOT NULL,   -- YYYY-MM
    txn_year            SMALLINT        NOT NULL,
    txn_month           TINYINT         NOT NULL,
    total_revenue       DECIMAL(16,2)   NOT NULL DEFAULT 0,
    total_cost          DECIMAL(16,2)   NOT NULL DEFAULT 0,
    total_gross_profit  DECIMAL(16,2)   NOT NULL DEFAULT 0,
    avg_profit_margin   DECIMAL(8,6)    NOT NULL DEFAULT 0,
    transaction_count   INT             NOT NULL DEFAULT 0,
    units_sold          INT             NOT NULL DEFAULT 0,
    refreshed_at        DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_monthly_revenue PRIMARY KEY CLUSTERED (month_label)
);

CREATE TABLE dbo.rpt_regional_summary (
    region              NVARCHAR(50)    NOT NULL,
    txn_year            SMALLINT        NOT NULL,
    total_revenue       DECIMAL(16,2)   NOT NULL DEFAULT 0,
    total_cost          DECIMAL(16,2)   NOT NULL DEFAULT 0,
    total_gross_profit  DECIMAL(16,2)   NOT NULL DEFAULT 0,
    avg_profit_margin   DECIMAL(8,6)    NOT NULL DEFAULT 0,
    transaction_count   INT             NOT NULL DEFAULT 0,
    refreshed_at        DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_regional_summary PRIMARY KEY CLUSTERED (region, txn_year)
);

CREATE TABLE dbo.rpt_product_summary (
    product_category    NVARCHAR(50)    NOT NULL,
    product_name        NVARCHAR(100)   NOT NULL,
    txn_year            SMALLINT        NOT NULL,
    total_revenue       DECIMAL(16,2)   NOT NULL DEFAULT 0,
    total_gross_profit  DECIMAL(16,2)   NOT NULL DEFAULT 0,
    avg_profit_margin   DECIMAL(8,6)    NOT NULL DEFAULT 0,
    units_sold          INT             NOT NULL DEFAULT 0,
    transaction_count   INT             NOT NULL DEFAULT 0,
    refreshed_at        DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_product_summary PRIMARY KEY CLUSTERED (product_name, txn_year)
);

CREATE TABLE dbo.rpt_anomalies (
    anomaly_id          INT             IDENTITY(1,1)   NOT NULL,
    transaction_id      NVARCHAR(20)    NOT NULL,
    txn_date            DATE            NOT NULL,
    product_name        NVARCHAR(100)   NOT NULL,
    region              NVARCHAR(50)    NOT NULL,
    revenue             DECIMAL(14,2)   NOT NULL,
    cost                DECIMAL(14,2)   NOT NULL,
    revenue_zscore      DECIMAL(8,4)    NULL,
    cost_zscore         DECIMAL(8,4)    NULL,
    anomaly_type        NVARCHAR(30)    NULL,   -- 'HIGH_REVENUE' | 'LOW_REVENUE' | 'HIGH_COST' | 'DISCOUNT_OUTLIER'
    detected_at         DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_rpt_anomalies PRIMARY KEY CLUSTERED (anomaly_id)
);

-- Generic reporting summary (consumed by pipeline_runner.py)
CREATE TABLE dbo.reporting_summary (
    summary_id          INT             IDENTITY(1,1)   NOT NULL,
    report_date         DATE            NOT NULL,
    entity_id           NVARCHAR(50)    NOT NULL,
    metric_name         NVARCHAR(100)   NOT NULL,
    total_value         DECIMAL(18,4)   NOT NULL,
    avg_value           DECIMAL(18,4)   NOT NULL,
    record_count        INT             NOT NULL,
    created_at          DATETIME2       NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_reporting_summary PRIMARY KEY CLUSTERED (summary_id)
);
GO

-- =============================================================================
-- INDEXES for dashboard query performance
-- =============================================================================

CREATE NONCLUSTERED INDEX IX_core_sales_date
    ON dbo.core_sales (txn_date)
    INCLUDE (revenue, gross_profit, product_category, region);

CREATE NONCLUSTERED INDEX IX_core_sales_year_month
    ON dbo.core_sales (txn_year, txn_month)
    INCLUDE (revenue, gross_profit, transaction_id);

CREATE NONCLUSTERED INDEX IX_core_sales_region
    ON dbo.core_sales (region)
    INCLUDE (txn_year, revenue, gross_profit);

CREATE NONCLUSTERED INDEX IX_core_sales_category
    ON dbo.core_sales (product_category)
    INCLUDE (txn_year, revenue, units_sold);

CREATE NONCLUSTERED INDEX IX_core_sales_channel
    ON dbo.core_sales (channel)
    INCLUDE (revenue, customer_segment);

CREATE NONCLUSTERED INDEX IX_core_sales_rep
    ON dbo.core_sales (sales_rep)
    INCLUDE (region, txn_year, revenue);

CREATE NONCLUSTERED INDEX IX_reporting_summary_date
    ON dbo.reporting_summary (report_date)
    INCLUDE (entity_id, metric_name, total_value);
GO

PRINT 'Schema creation complete — bi_reporting_db objects created successfully.';
GO
-- =============================================================================
-- END OF SCHEMA
-- =============================================================================
