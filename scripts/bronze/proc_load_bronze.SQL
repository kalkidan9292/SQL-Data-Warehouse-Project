/* ====================================================================================================
   Stored Procedure: Load Bronze Layer  (Source → Bronze)
   ----------------------------------------------------------------------------------------------------
   Purpose:
     Loads raw source CSV files into the 'bronze' schema as part of the Medallion Architecture pipeline.
     This procedure performs:
       - Truncation of Bronze tables before loading.
       - BULK INSERT operations from external CSV files.
       - Duration tracking for each table load and total batch timing.
       - Error handling with detailed diagnostics.

   Parameters:
     None — this procedure does not accept parameters.

   Usage:
     EXEC bronze.load_bronze;

   ==================================================================================================== */


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '========================================';
        PRINT 'Loading Bronze Layer';
        PRINT '========================================';

        PRINT '----------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '----------------------------------------';

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        
        PRINT '>> Inserting Data into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\kalki\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('>> -----------------------------------------------------------------------------------------');

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\kalki\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('>> -----------------------------------------------------------------------------------------');

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\kalki\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('>> -----------------------------------------------------------------------------------------');

        PRINT '----------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '----------------------------------------';

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\kalki\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('>> -----------------------------------------------------------------------------------------');

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\kalki\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('>> -----------------------------------------------------------------------------------------');

        SET @start_time = GETDATE();

        PRINT '>> Truncating Table: bronze.erp_cat_g1v2';
        TRUNCATE TABLE bronze.erp_cat_g1v2;

        PRINT '>> Inserting Data into: bronze.erp_cat_g1v2';
        BULK INSERT bronze.erp_cat_g1v2
        FROM 'C:\Users\kalki\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @end_time = GETDATE();
        PRINT('>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds');
        PRINT('>> -----------------------------------------------------------------------------------------');

        SET @batch_end_time =GETDATE();
        PRINT '=========================================================';
        PRINT 'Loading Bronze Layer is Completed';
        PRINT ' - Total Load Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)+ ' seconds';
        PRINT '=========================================================';


    END TRY

    BEGIN CATCH
        PRINT '=========================================================';
        PRINT '====== ERROR OCCURRED DURING LOADING BRONZE LAYER =======';
        PRINT '==== ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT '==== ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT '==== ERROR STATE : ' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '=========================================================';
    END CATCH
END;
