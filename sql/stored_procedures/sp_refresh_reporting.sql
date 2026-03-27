-- =============================================================================
-- usp_DailyDataExtraction.sql
-- Automated Business Intelligence Reporting System
-- Platform  : MS SQL Server 2019+ (T-SQL)
-- Author    : Olayinka Somuyiwa
-- Purpose   : Automated daily data extraction — extracts and stages daily
--             operational data for downstream transformation and reporting.
-- Schedule  : SQL Server Agent Job — daily at 06:00
-- =============================================================================

USE [bi_reporting_db];
GO

CREATE OR ALTER PROCEDURE dbo.usp_DailyDataExtraction
    @extraction_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SET @extraction_date = ISNULL(@extraction_date, CAST(GETDATE() AS DATE));

    -- Log extraction start
    INSERT INTO dbo.pipeline_log (run_date, stage, status, message)
    VALUES (
        @extraction_date,
        'Extraction',
        'Started',
        'Daily extraction initiated for ' + CAST(@extraction_date AS VARCHAR)
    );

    BEGIN TRY

        -- Extract and stage operational data
        INSERT INTO dbo.staging_operational_data
            (record_date, entity_id, metric_name, metric_value, source_system)
        SELECT
            @extraction_date,
            entity_id,
            metric_name,
            metric_value,
            source_system
        FROM dbo.vw_operational_source
        WHERE record_date = @extraction_date;

        -- Log completion with record count
        UPDATE dbo.pipeline_log
        SET
            status            = 'Completed',
            records_processed = @@ROWCOUNT,
            end_time          = GETDATE()
        WHERE run_date = @extraction_date
          AND stage    = 'Extraction';

    END TRY
    BEGIN CATCH

        UPDATE dbo.pipeline_log
        SET
            status        = 'Failed',
            end_time      = SYSDATETIME(),
            error_message = ERROR_MESSAGE()
        WHERE run_date = @extraction_date
          AND stage    = 'Extraction'
          AND status   = 'Started';

        THROW;

    END CATCH;

END;
GO

-- =============================================================================
-- usp_TransformationLayer.sql
-- Runs the full staging → core → reporting transformation sequence.
-- Called by SQL Server Agent after usp_DailyDataExtraction completes.
-- =============================================================================

CREATE OR ALTER PROCEDURE dbo.usp_TransformationLayer
    @run_date DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET @run_date = ISNULL(@run_date, CAST(GETDATE() AS DATE));

    BEGIN TRY

        -- ── Step 1: Staging clean ──────────────────────────────────────────

        INSERT INTO dbo.pipeline_log (run_date, stage, status, message)
        VALUES (@run_date, 'Staging', 'Started', 'Staging validation initiated');

        -- Deduplication
        WITH cte_dupes AS (
            SELECT transaction_id,
                   ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY loaded_at) AS rn
            FROM dbo.stg_sales_raw
        )
        DELETE FROM dbo.stg_sales_raw
        WHERE transaction_id IN (SELECT transaction_id FROM cte_dupes WHERE rn > 1);

        -- Remove invalid rows
        DELETE FROM dbo.stg_sales_raw
        WHERE transaction_id IS NULL OR record_date IS NULL OR revenue < 0;

        -- Clamp discount
        UPDATE dbo.stg_sales_raw
        SET discount_pct = CASE
            WHEN discount_pct < 0   THEN 0
            WHEN discount_pct > 100 THEN 100
            ELSE discount_pct END;

        UPDATE dbo.pipeline_log
        SET status = 'Completed', end_time = SYSDATETIME(),
            records_processed = (SELECT COUNT(*) FROM dbo.stg_sales_raw)
        WHERE run_date = @run_date AND stage = 'Staging' AND status = 'Started';

        -- ── Step 2: Core transformation ───────────────────────────────────

        INSERT INTO dbo.pipeline_log (run_date, stage, status, message)
        VALUES (@run_date, 'Transformation', 'Started', 'Core layer transformation initiated');

        TRUNCATE TABLE dbo.core_sales;

        INSERT INTO dbo.core_sales (
            transaction_id, txn_date, txn_year, txn_quarter, txn_month,
            txn_month_label, product_name, product_category, region, sales_rep,
            customer_id, customer_segment, channel, units_sold, unit_price,
            revenue, cost, gross_profit, profit_margin, discount_pct,
            discount_amount, is_profitable
        )
        SELECT
            s.transaction_id,
            s.record_date,
            YEAR(s.record_date),
            DATEPART(QUARTER, s.record_date),
            MONTH(s.record_date),
            FORMAT(s.record_date, 'yyyy-MM'),
            LTRIM(RTRIM(s.product_name)),
            LTRIM(RTRIM(s.product_category)),
            LTRIM(RTRIM(s.region)),
            LTRIM(RTRIM(s.sales_rep)),
            LTRIM(RTRIM(s.customer_id)),
            LTRIM(RTRIM(s.customer_segment)),
            LTRIM(RTRIM(s.channel)),
            s.units_sold,
            s.unit_price,
            ROUND(s.revenue, 2),
            ROUND(s.cost, 2),
            ROUND(s.revenue - s.cost, 2),
            ROUND(ISNULL((s.revenue - s.cost) / NULLIF(s.revenue, 0), 0), 6),
            s.discount_pct,
            ROUND(s.unit_price * s.units_sold * (s.discount_pct / 100.0), 2),
            CASE WHEN (s.revenue - s.cost) > 0 THEN 1 ELSE 0 END
        FROM dbo.stg_sales_raw s
        WHERE s.transaction_id IS NOT NULL
          AND s.record_date    IS NOT NULL
          AND s.revenue        IS NOT NULL;

        UPDATE dbo.pipeline_log
        SET status = 'Completed', end_time = SYSDATETIME(),
            records_processed = (SELECT COUNT(*) FROM dbo.core_sales)
        WHERE run_date = @run_date AND stage = 'Transformation' AND status = 'Started';

        -- ── Step 3: Reporting aggregation ─────────────────────────────────

        INSERT INTO dbo.pipeline_log (run_date, stage, status, message)
        VALUES (@run_date, 'Reporting', 'Started', 'Reporting aggregation initiated');

        TRUNCATE TABLE dbo.rpt_monthly_revenue;
        INSERT INTO dbo.rpt_monthly_revenue (month_label, txn_year, txn_month,
            total_revenue, total_cost, total_gross_profit, avg_profit_margin,
            transaction_count, units_sold)
        SELECT txn_month_label, txn_year, txn_month,
            ROUND(SUM(revenue),2), ROUND(SUM(cost),2), ROUND(SUM(gross_profit),2),
            ROUND(AVG(profit_margin),6), COUNT(*), SUM(units_sold)
        FROM dbo.core_sales GROUP BY txn_month_label, txn_year, txn_month;

        TRUNCATE TABLE dbo.rpt_regional_summary;
        INSERT INTO dbo.rpt_regional_summary (region, txn_year,
            total_revenue, total_cost, total_gross_profit, avg_profit_margin, transaction_count)
        SELECT region, txn_year,
            ROUND(SUM(revenue),2), ROUND(SUM(cost),2), ROUND(SUM(gross_profit),2),
            ROUND(AVG(profit_margin),6), COUNT(*)
        FROM dbo.core_sales GROUP BY region, txn_year;

        TRUNCATE TABLE dbo.rpt_product_summary;
        INSERT INTO dbo.rpt_product_summary (product_category, product_name, txn_year,
            total_revenue, total_gross_profit, avg_profit_margin, units_sold, transaction_count)
        SELECT product_category, product_name, txn_year,
            ROUND(SUM(revenue),2), ROUND(SUM(gross_profit),2),
            ROUND(AVG(profit_margin),6), SUM(units_sold), COUNT(*)
        FROM dbo.core_sales GROUP BY product_category, product_name, txn_year;

        UPDATE dbo.pipeline_log
        SET status = 'Completed', end_time = SYSDATETIME(),
            records_processed = (SELECT COUNT(*) FROM dbo.rpt_monthly_revenue)
        WHERE run_date = @run_date AND stage = 'Reporting' AND status = 'Started';

        PRINT 'Full pipeline transformation complete for ' + CAST(@run_date AS VARCHAR);

    END TRY
    BEGIN CATCH

        UPDATE dbo.pipeline_log
        SET status = 'Failed', end_time = SYSDATETIME(), error_message = ERROR_MESSAGE()
        WHERE run_date = @run_date AND status = 'Started';

        THROW;

    END CATCH;

END;
GO

-- =============================================================================
-- EXECUTION — Uncomment to run manually in SSMS
-- =============================================================================

-- Run extraction for today
-- EXEC dbo.usp_DailyDataExtraction;

-- Run full transformation pipeline
-- EXEC dbo.usp_TransformationLayer;

-- View pipeline log
-- SELECT * FROM dbo.pipeline_log ORDER BY log_id DESC;

PRINT 'Stored procedures created: usp_DailyDataExtraction, usp_TransformationLayer';
GO

-- =============================================================================
-- SQL SERVER AGENT JOB SETUP (run in SSMS after procedures are created)
-- =============================================================================

/*
-- Step 1: Create the Agent Job
EXEC msdb.dbo.sp_add_job
    @job_name = N'BI_Pipeline_Daily_Run';

-- Step 2: Add extraction step (06:00)
EXEC msdb.dbo.sp_add_jobstep
    @job_name  = N'BI_Pipeline_Daily_Run',
    @step_name = N'Step1_DailyExtraction',
    @command   = N'EXEC dbo.usp_DailyDataExtraction;',
    @database_name = N'bi_reporting_db';

-- Step 3: Add transformation step
EXEC msdb.dbo.sp_add_jobstep
    @job_name  = N'BI_Pipeline_Daily_Run',
    @step_name = N'Step2_Transformation',
    @command   = N'EXEC dbo.usp_TransformationLayer;',
    @database_name = N'bi_reporting_db';

-- Step 4: Schedule daily at 06:00
EXEC msdb.dbo.sp_add_schedule
    @schedule_name     = N'DailyAt0600',
    @freq_type         = 4,           -- Daily
    @freq_interval     = 1,
    @active_start_time = 060000;      -- 06:00:00

EXEC msdb.dbo.sp_attach_schedule
    @job_name      = N'BI_Pipeline_Daily_Run',
    @schedule_name = N'DailyAt0600';

EXEC msdb.dbo.sp_add_jobserver
    @job_name = N'BI_Pipeline_Daily_Run';
*/
GO
-- =============================================================================
-- END OF STORED PROCEDURES
-- =============================================================================
