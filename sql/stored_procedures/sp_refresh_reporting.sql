-- =============================================================================
-- sp_refresh_reporting.sql
-- Enterprise BI Reporting System — Stored Procedure: Full Pipeline Refresh
--
-- SQL Server syntax (T-SQL). For SQLite, use Python sql_runner.py instead.
-- Uncomment and run in SQL Server Management Studio (SSMS).
-- =============================================================================

/*
USE [BIReportingDB];
GO

-- Drop if exists
IF OBJECT_ID('dbo.sp_refresh_reporting', 'P') IS NOT NULL
    DROP PROCEDURE dbo.sp_refresh_reporting;
GO

CREATE PROCEDURE dbo.sp_refresh_reporting
    @RunDate        DATE        = NULL,     -- Override run date (default: today)
    @LogToTable     BIT         = 1,        -- Log execution to audit table
    @FullRefresh    BIT         = 1         -- Full or incremental refresh
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @StartTime      DATETIME2   = SYSDATETIME();
    DECLARE @RunDateFinal   DATE        = COALESCE(@RunDate, CAST(GETDATE() AS DATE));
    DECLARE @StepName       NVARCHAR(100);
    DECLARE @RowsAffected   INT;
    DECLARE @ErrorMsg       NVARCHAR(MAX);

    BEGIN TRY
        -- ─────────────────────────────────────────────────────────────────────
        -- STEP 1: Refresh Staging Layer
        -- ─────────────────────────────────────────────────────────────────────
        SET @StepName = 'Staging Layer Refresh';

        IF @FullRefresh = 1
            TRUNCATE TABLE dbo.stg_sales_raw;

        -- In production, this would call BULK INSERT or OPENROWSET
        -- BULK INSERT dbo.stg_sales_raw
        --   FROM 'C:\data\sales_operations.csv'
        --   WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n');

        SET @RowsAffected = @@ROWCOUNT;

        -- ─────────────────────────────────────────────────────────────────────
        -- STEP 2: Core Layer Transformation
        -- ─────────────────────────────────────────────────────────────────────
        SET @StepName = 'Core Layer Transformation';

        IF @FullRefresh = 1
            TRUNCATE TABLE dbo.core_sales;

        INSERT INTO dbo.core_sales (
            transaction_id, txn_date, txn_year, txn_quarter, txn_month,
            txn_month_label, product_name, product_category, region,
            sales_rep, customer_id, customer_segment, channel,
            units_sold, unit_price, revenue, cost, gross_profit,
            profit_margin, discount_pct, discount_amount, is_profitable
        )
        SELECT
            transaction_id,
            CAST(date AS DATE)                              AS txn_date,
            YEAR(CAST(date AS DATE))                        AS txn_year,
            DATEPART(QUARTER, CAST(date AS DATE))           AS txn_quarter,
            MONTH(CAST(date AS DATE))                       AS txn_month,
            FORMAT(CAST(date AS DATE), 'yyyy-MM')           AS txn_month_label,
            TRIM(product_name),
            TRIM(product_category),
            TRIM(region),
            TRIM(sales_rep),
            TRIM(customer_id),
            TRIM(customer_segment),
            TRIM(channel),
            units_sold,
            unit_price,
            ROUND(revenue, 2),
            ROUND(cost, 2),
            ROUND(revenue - cost, 2),
            CASE WHEN revenue = 0 THEN 0
                 ELSE ROUND((revenue - cost) / revenue, 6) END,
            discount_pct,
            ROUND(unit_price * units_sold * (discount_pct / 100.0), 2),
            CASE WHEN (revenue - cost) > 0 THEN 1 ELSE 0 END
        FROM dbo.stg_sales_raw
        WHERE transaction_id IS NOT NULL
          AND date IS NOT NULL
          AND revenue IS NOT NULL;

        SET @RowsAffected = @@ROWCOUNT;

        -- ─────────────────────────────────────────────────────────────────────
        -- STEP 3: Reporting Layer Aggregation
        -- ─────────────────────────────────────────────────────────────────────
        SET @StepName = 'Reporting Layer — Monthly';

        TRUNCATE TABLE dbo.rpt_monthly_revenue;

        INSERT INTO dbo.rpt_monthly_revenue
        SELECT
            txn_month_label,
            txn_year,
            txn_month,
            ROUND(SUM(revenue), 2),
            ROUND(SUM(cost), 2),
            ROUND(SUM(gross_profit), 2),
            ROUND(AVG(profit_margin), 6),
            COUNT(*),
            SUM(units_sold)
        FROM dbo.core_sales
        GROUP BY txn_month_label, txn_year, txn_month;

        -- Regional Summary
        SET @StepName = 'Reporting Layer — Regional';
        TRUNCATE TABLE dbo.rpt_regional_summary;

        INSERT INTO dbo.rpt_regional_summary
        SELECT
            region, txn_year,
            ROUND(SUM(revenue), 2),
            ROUND(SUM(cost), 2),
            ROUND(SUM(gross_profit), 2),
            ROUND(AVG(profit_margin), 6),
            COUNT(*)
        FROM dbo.core_sales
        GROUP BY region, txn_year;

        -- Product Summary
        SET @StepName = 'Reporting Layer — Product';
        TRUNCATE TABLE dbo.rpt_product_summary;

        INSERT INTO dbo.rpt_product_summary
        SELECT
            product_category, product_name, txn_year,
            ROUND(SUM(revenue), 2),
            ROUND(SUM(gross_profit), 2),
            ROUND(AVG(profit_margin), 6),
            SUM(units_sold),
            COUNT(*)
        FROM dbo.core_sales
        GROUP BY product_category, product_name, txn_year;

        -- ─────────────────────────────────────────────────────────────────────
        -- STEP 4: Audit Log
        -- ─────────────────────────────────────────────────────────────────────
        IF @LogToTable = 1
        BEGIN
            INSERT INTO dbo.pipeline_audit_log (
                run_date, pipeline_name, status, rows_processed,
                duration_seconds, completed_at
            )
            VALUES (
                @RunDateFinal,
                'sp_refresh_reporting',
                'SUCCESS',
                @RowsAffected,
                DATEDIFF(SECOND, @StartTime, SYSDATETIME()),
                SYSDATETIME()
            );
        END;

        PRINT 'Pipeline completed successfully. Duration: '
              + CAST(DATEDIFF(SECOND, @StartTime, SYSDATETIME()) AS VARCHAR) + 's';

    END TRY
    BEGIN CATCH
        SET @ErrorMsg = ERROR_MESSAGE();

        IF @LogToTable = 1
        BEGIN
            INSERT INTO dbo.pipeline_audit_log (
                run_date, pipeline_name, status, error_message, completed_at
            )
            VALUES (
                @RunDateFinal, 'sp_refresh_reporting',
                'FAILED', @ErrorMsg, SYSDATETIME()
            );
        END;

        RAISERROR (@ErrorMsg, 16, 1);
        RETURN -1;
    END CATCH;

    RETURN 0;
END;
GO

-- ── EXECUTE ──────────────────────────────────────────────────────────────────
-- EXEC dbo.sp_refresh_reporting @FullRefresh = 1, @LogToTable = 1;
-- GO
*/

-- SQLite equivalent (no stored procedure support — handled in Python sql_runner.py)
-- See: python/transformation/sql_runner.py → run_all_transformations()
SELECT 'SQL Server stored procedure defined above. For SQLite, use sql_runner.py.' AS note;
