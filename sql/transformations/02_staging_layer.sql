-- =============================================================================
-- 02_staging_layer.sql
-- Automated Business Intelligence Reporting System
-- Platform : MS SQL Server 2019+ (T-SQL)
-- Author   : Olayinka Somuyiwa
-- Purpose  : Validate and clean the staging table after CSV bulk load.
--            Deduplication, null handling, categorical standardization.
--            No business logic — this layer mirrors the source exactly.
-- =============================================================================

USE [bi_reporting_db];
GO

-- =============================================================================
-- VALIDATION REPORT — written to pipeline_log before cleaning
-- =============================================================================

DECLARE @run_date DATE = CAST(GETDATE() AS DATE);

INSERT INTO dbo.pipeline_log (run_date, stage, status, message)
VALUES (@run_date, 'Staging', 'Started', 'Staging validation and clean initiated');

-- Validation summary (query results visible in SSMS output)
SELECT
    'NULL_TRANSACTION_ID'   AS check_name,
    COUNT(*)                AS affected_rows
FROM dbo.stg_sales_raw
WHERE transaction_id IS NULL OR LTRIM(RTRIM(transaction_id)) = ''

UNION ALL SELECT 'NULL_RECORD_DATE',    COUNT(*) FROM dbo.stg_sales_raw WHERE record_date IS NULL
UNION ALL SELECT 'NULL_REVENUE',        COUNT(*) FROM dbo.stg_sales_raw WHERE revenue IS NULL
UNION ALL SELECT 'NEGATIVE_REVENUE',    COUNT(*) FROM dbo.stg_sales_raw WHERE revenue < 0
UNION ALL SELECT 'NEGATIVE_COST',       COUNT(*) FROM dbo.stg_sales_raw WHERE cost < 0
UNION ALL SELECT 'INVALID_DISCOUNT',    COUNT(*) FROM dbo.stg_sales_raw WHERE discount_pct < 0 OR discount_pct > 100
UNION ALL SELECT 'ZERO_UNITS',          COUNT(*) FROM dbo.stg_sales_raw WHERE units_sold <= 0
UNION ALL SELECT 'DUPLICATE_TXN_IDS',   COUNT(*) - COUNT(DISTINCT transaction_id) FROM dbo.stg_sales_raw;

-- =============================================================================
-- DEDUPLICATION — keep first loaded record per transaction_id
-- =============================================================================

WITH cte_dupes AS (
    SELECT
        transaction_id,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY loaded_at ASC
        ) AS rn
    FROM dbo.stg_sales_raw
)
DELETE FROM dbo.stg_sales_raw
WHERE transaction_id IN (
    SELECT transaction_id FROM cte_dupes WHERE rn > 1
);

-- =============================================================================
-- REMOVE CRITICAL NULL / INVALID ROWS
-- =============================================================================

DELETE FROM dbo.stg_sales_raw
WHERE transaction_id IS NULL
   OR LTRIM(RTRIM(transaction_id)) = ''
   OR record_date IS NULL
   OR revenue IS NULL
   OR revenue < 0;

-- =============================================================================
-- AUTO-FIX NON-CRITICAL ISSUES
-- =============================================================================

-- Clamp discount to valid 0–100% range
UPDATE dbo.stg_sales_raw
SET discount_pct = CASE
    WHEN discount_pct < 0   THEN 0
    WHEN discount_pct > 100 THEN 100
    ELSE discount_pct
END;

-- Default zero/null units to 1
UPDATE dbo.stg_sales_raw
SET units_sold = 1
WHERE units_sold IS NULL OR units_sold <= 0;

-- Default null cost to 0
UPDATE dbo.stg_sales_raw
SET cost = 0
WHERE cost IS NULL;

-- =============================================================================
-- STANDARDIZE CATEGORICALS (title-case)
-- =============================================================================

UPDATE dbo.stg_sales_raw
SET region = CASE LOWER(LTRIM(RTRIM(region)))
    WHEN 'north'         THEN 'North'
    WHEN 'south'         THEN 'South'
    WHEN 'east'          THEN 'East'
    WHEN 'west'          THEN 'West'
    WHEN 'international' THEN 'International'
    ELSE LTRIM(RTRIM(region))
END;

UPDATE dbo.stg_sales_raw
SET channel = CASE LOWER(LTRIM(RTRIM(channel)))
    WHEN 'direct'  THEN 'Direct'
    WHEN 'partner' THEN 'Partner'
    WHEN 'online'  THEN 'Online'
    ELSE LTRIM(RTRIM(channel))
END;

UPDATE dbo.stg_sales_raw
SET customer_segment = CASE LOWER(LTRIM(RTRIM(customer_segment)))
    WHEN 'enterprise' THEN 'Enterprise'
    WHEN 'smb'        THEN 'SMB'
    WHEN 'consumer'   THEN 'Consumer'
    ELSE LTRIM(RTRIM(customer_segment))
END;

-- =============================================================================
-- STAGING SUMMARY — final row count after cleaning
-- =============================================================================

DECLARE @staging_count INT = (SELECT COUNT(*) FROM dbo.stg_sales_raw);

UPDATE dbo.pipeline_log
SET
    status            = 'Completed',
    records_processed = @staging_count,
    end_time          = SYSDATETIME(),
    message           = 'Staging clean complete — ' + CAST(@staging_count AS NVARCHAR) + ' rows validated'
WHERE run_date = @run_date AND stage = 'Staging' AND status = 'Started';

SELECT
    'STAGING_SUMMARY'                       AS layer,
    COUNT(*)                                AS total_rows,
    MIN(record_date)                        AS earliest_date,
    MAX(record_date)                        AS latest_date,
    CAST(SUM(revenue) AS DECIMAL(16,2))     AS total_revenue,
    CAST(AVG(revenue) AS DECIMAL(14,2))     AS avg_revenue,
    COUNT(DISTINCT region)                  AS region_count,
    COUNT(DISTINCT product_name)            AS product_count
FROM dbo.stg_sales_raw;
GO

-- =============================================================================
-- END OF STAGING LAYER
-- =============================================================================
