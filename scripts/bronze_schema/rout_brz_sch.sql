/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This Routine (OR procedure) loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.


This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
    
Note: Here path to CSV files is from my own local directory (in Linux).
For Windows users, you may use C:/,D:,...etc
===============================================================================
*/
CREATE PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;
    SET NOCOUNT ON;

    BEGIN TRY

        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT '================================================';

        /* ================= CRM TABLES ================= */

        PRINT '================================================';
        PRINT 'Loading CRM Tables';
        PRINT '================================================';

        -- 1. Customer Info
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Loading: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
            FROM '/home/[USER]/Documents/Project/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>-------------------';

        /* ================= PRODUCT INFO (WITH STAGING) ================= */

        SET @start_time = GETDATE();
        IF OBJECT_ID('stg_prd_info', 'U') IS NOT NULL
            DROP TABLE stg_prd_info;

        PRINT '>> Creating staging table: stg_prd_info';
        CREATE TABLE stg_prd_info (
            prd_id VARCHAR(50),
            prd_key VARCHAR(50),
            prd_nm VARCHAR(255),
            prd_cost VARCHAR(50),
            prd_line VARCHAR(10),
            prd_start_dt VARCHAR(50),
            prd_end_dt VARCHAR(50)
        );

        PRINT '>> Loading staging: stg_prd_info';
        BULK INSERT stg_prd_info
            FROM '/home/[USER]/Documents/Project/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '0x0a',
            TABLOCK
            );

        PRINT '>> Truncating: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Transforming & Loading: bronze.crm_prd_info';
        INSERT INTO bronze.crm_prd_info (
            prd_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT
            TRY_CAST(prd_id AS INT),
            prd_key,
            LTRIM(RTRIM(prd_nm)),
            TRY_CAST(prd_cost AS INT),
            NULLIF(prd_line, ''),
            TRY_CAST(prd_start_dt AS DATETIME),
            CASE
                WHEN prd_end_dt = '' THEN NULL
                WHEN TRY_CAST(prd_end_dt AS DATETIME) < TRY_CAST(prd_start_dt AS DATETIME)
                    THEN NULL
                ELSE TRY_CAST(prd_end_dt AS DATETIME)
            END
        FROM stg_prd_info;
        
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>-------------------';

        /* ================= SALES ================= */

        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Loading: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
            FROM '/home/[USER]/Documents/Project/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>-------------------';

        /* ================= ERP TABLES ================= */

        PRINT '================================================';
        PRINT 'Loading ERP Tables';
        PRINT '================================================';

        -- ERP Customer
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Loading: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
            FROM '/home/[USER]/Documents/Project/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>-------------------';

        -- ERP Location
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Loading: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
            FROM '/home/[USER]/Documents/Project/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>-------------------';

        -- ERP Product Category
        SET @start_time = GETDATE();
        PRINT '>> Truncating: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Loading: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
            FROM '/home/[USER]/Documents/Project/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
            WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
            );
        
        SET @end_time = GETDATE();
        PRINT '>>Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>-------------------';

        PRINT '================================================';
        PRINT 'Bronze Layer Load Completed Successfully';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '===============================================';
        PRINT 'ERROR OCCURRED DURING LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '===============================================';
    END CATCH
END;
GO
