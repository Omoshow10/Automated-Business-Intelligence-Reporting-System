-- =============================================================================
-- 02_staging_layer.sql
-- Enterprise BI Reporting System — Raw → Staging Transformation
--
-- Purpose:
--   Load CSV data into the staging table with basic validation.
--   Deduplication and null handling happens here.
--   No business logic — this layer mirrors the source.
-- =============================================================================

-- Clear existing staging data (full refresh pattern)
DELETE FROM stg_sales_raw;

-- Load from CSV is handled by Python (data_loader.py).
-- This script validates and cleans the staging table after load.

-- -----------------------------------------------------------------------------
-- VALIDATION CHECKS — log issues before transformation
-- (In production these would write to an audit/log table)
-- -----------------------------------------------------------------------------

-- Check: Missing required fields
SELECT
    'VALIDATION' AS check_type,
    'NULL_TRANSACTION_ID' AS issue,
    COUNT(*) AS affected_rows
FROM stg_sales_raw
WHERE transaction_id IS NULL OR TRIM(transaction_id) = ''

UNION ALL

SELECT 'VALIDATION', 'NULL_DATE',         COUNT(*) FROM stg_sales_raw WHERE date IS NULL
UNION ALL
SELECT 'VALIDATION', 'NULL_REVENUE',      COUNT(*) FROM stg_sales_raw WHERE revenue IS NULL
UNION ALL
SELECT 'VALIDATION', 'NEGATIVE_REVENUE',  COUNT(*) FROM stg_sales_raw WHERE revenue < 0
UNION ALL
SELECT 'VALIDATION', 'NEGATIVE_COST',     COUNT(*) FROM stg_sales_raw WHERE cost < 0
UNION ALL
SELECT 'VALIDATION', 'INVALID_DISCOUNT',  COUNT(*) FROM stg_sales_raw WHERE discount_pct < 0 OR discount_pct > 100
UNION ALL
SELECT 'VALIDATION', 'ZERO_UNITS',        COUNT(*) FROM stg_sales_raw WHERE units_sold <= 0
UNION ALL
SELECT 'VALIDATION', 'DUPLICATE_TXN_IDS', COUNT(*) - COUNT(DISTINCT transaction_id) FROM stg_sales_raw;

-- -----------------------------------------------------------------------------
-- CLEAN STAGING DATA IN PLACE
-- Remove duplicates (keep first occurrence by rowid)
-- -----------------------------------------------------------------------------

DELETE FROM stg_sales_raw
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM stg_sales_raw
    GROUP BY transaction_id
);

-- Remove rows with critical null values
DELETE FROM stg_sales_raw
WHERE transaction_id IS NULL
   OR date IS NULL
   OR revenue IS NULL
   OR cost IS NULL;

-- Remove rows with negative revenue (data quality issue)
DELETE FROM stg_sales_raw
WHERE revenue < 0;

-- Clamp discount to valid range 0–100%
UPDATE stg_sales_raw
SET discount_pct = CASE
    WHEN discount_pct < 0   THEN 0
    WHEN discount_pct > 100 THEN 100
    ELSE discount_pct
END;

-- Set missing units_sold to 1
UPDATE stg_sales_raw
SET units_sold = 1
WHERE units_sold IS NULL OR units_sold <= 0;

-- Standardize region casing
UPDATE stg_sales_raw
SET region = CASE LOWER(TRIM(region))
    WHEN 'north'         THEN 'North'
    WHEN 'south'         THEN 'South'
    WHEN 'east'          THEN 'East'
    WHEN 'west'          THEN 'West'
    WHEN 'international' THEN 'International'
    ELSE region
END;

-- Standardize channel casing
UPDATE stg_sales_raw
SET channel = CASE LOWER(TRIM(channel))
    WHEN 'direct'  THEN 'Direct'
    WHEN 'partner' THEN 'Partner'
    WHEN 'online'  THEN 'Online'
    ELSE channel
END;

-- Standardize customer_segment casing
UPDATE stg_sales_raw
SET customer_segment = CASE LOWER(TRIM(customer_segment))
    WHEN 'enterprise' THEN 'Enterprise'
    WHEN 'smb'        THEN 'SMB'
    WHEN 'consumer'   THEN 'Consumer'
    ELSE customer_segment
END;

-- Summary count after staging clean
SELECT
    'STAGING_SUMMARY' AS layer,
    COUNT(*)          AS total_rows,
    MIN(date)         AS earliest_date,
    MAX(date)         AS latest_date,
    ROUND(SUM(revenue), 2)      AS total_revenue,
    ROUND(AVG(revenue), 2)      AS avg_revenue,
    COUNT(DISTINCT region)      AS region_count,
    COUNT(DISTINCT product_name) AS product_count
FROM stg_sales_raw;

-- =============================================================================
-- END OF STAGING LAYER
-- =============================================================================
