/*
============================================================
Stored Procedure: Load Silver Layer (Bronze → Silver)
============================================================

Script Purpose:
    This stored procedure performs the ETL process to load 
    the Silver Layer with cleaned, standardized, and 
    business-ready data from the Bronze Layer.

Actions Performed:
    - Truncates Silver tables before loading.
    - Applies data quality rules (validation, cleaning, fixing).
    - Standardizes codes, dates, product lines, and customer attributes.
    - Handles NULLs, incorrect values, and transformations.
    - Loads curated Silver tables for analytics and reporting.
    - Logs the duration of each step and the total load time.

Parameters:
    None.
    This stored procedure does not accept parameters or 
    return values.

Usage:
    EXEC silver.load_silver;

============================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE 
        @start_time        DATETIME,
        @end_time          DATETIME,
        @batch_start_time  DATETIME,
        @batch_end_time    DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '========================================================';
        PRINT 'Starting Silver Layer Load';
        PRINT '========================================================';

/*  
    ======================= CRM CUSTOMER =======================
    Deduplicates customer records, cleans names, normalizes
    marital status & gender, and loads the latest customer record.
*/
        SET @start_time = GETDATE();
        PRINT '----------------------------------------';
        PRINT 'Loading silver.crm_cust_info';
        PRINT '----------------------------------------';

        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;

        PRINT '>> Inserting Data Info: silver.crm_cust_info';
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_material_status,
            cst_gndr,
            cst_create_date
        )
        SELECT 
            cst_id,
            cst_key, 
            TRIM(cst_firstname) AS cst_firstname, 
            TRIM(cst_lastname) AS cst_lastname, 
            CASE 
                WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
                ELSE 'Unknown'
            END AS cst_material_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'Unknown'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                        PARTITION BY cst_id 
                        ORDER BY cst_create_date DESC
                   ) AS flag_last
            FROM bronze.crm_cust_info
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Step Duration (silver.crm_cust_info): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) 
              + ' seconds';
        PRINT '>> ----------------------------------------------------';

/*  
    ======================= CRM PRODUCT ========================
    Extracts category ID, standardizes product line values,
    fixes NULL cost values, and calculates product end dates.
*/
        SET @start_time = GETDATE();
        PRINT '----------------------------------------';
        PRINT 'Loading silver.crm_prd_info';
        PRINT '----------------------------------------';

        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;

        PRINT '>> Inserting Data Info: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info (
            prd_id,
            cat_id,
            prd_key,
            prd_nm,
            prd_cost,
            prd_line,
            prd_start_dt,
            prd_end_dt
        )
        SELECT 
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
            prd_nm,
            ISNULL(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line)) 
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'Unknown'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                DATEADD(
                    DAY, -1,
                    LEAD(prd_start_dt) OVER (
                        PARTITION BY prd_key 
                        ORDER BY prd_start_dt
                    )
                ) AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Step Duration (silver.crm_prd_info): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) 
              + ' seconds';
        PRINT '>> ----------------------------------------------------';

/*  
    ======================= CRM SALES ==========================
    Converts YYYYMMDD integers to DATE, recalculates incorrect
    sales totals, fixes invalid price values, and loads cleansed
    sales transactions.
*/
        SET @start_time = GETDATE();
        PRINT '----------------------------------------';
        PRINT 'Loading silver.crm_sales_details';
        PRINT '----------------------------------------';

        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;

        PRINT '>> Inserting Data Info: silver.crm_sales_details';
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_ord_key,
            sls_cust_id,
            sls_order_dt,      
            sls_ship_dt,      
            sls_due_dt,      
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT 
            sls_ord_num,
            sls_ord_key AS sls_prd_key,
            sls_cust_id,

            CASE 
                WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE) 
            END AS sls_order_dt,

            CASE 
                WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE) 
            END AS sls_ship_dt,

            CASE 
                WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
                ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE) 
            END AS sls_due_dt,

            CASE 
                WHEN sls_sales IS NULL 
                     OR sls_sales <= 0
                     OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,

            sls_quantity,

            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN CAST(sls_sales AS DECIMAL(10,2)) / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Step Duration (silver.crm_sales_details): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) 
              + ' seconds';
        PRINT '>> ----------------------------------------------------';

/*  
    ======================= ERP CUSTOMER =======================
    Cleans customer IDs, fixes invalid future birthdates, and
    standardizes gender labels from the ERP source.
*/
        SET @start_time = GETDATE();
        PRINT '----------------------------------------';
        PRINT 'Loading silver.erp_cust_az12';
        PRINT '----------------------------------------';

        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;

        PRINT '>> Inserting Data Info: silver.erp_cust_az12';
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE 
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
                ELSE cid
            END AS cid,
            CASE 
                WHEN bdate > GETDATE() THEN NULL
                ELSE bdate
            END AS bdate,
            CASE 
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'Unknown'
            END AS gen
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Step Duration (silver.erp_cust_az12): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) 
              + ' seconds';
        PRINT '>> ----------------------------------------------------';

/*  
    ======================= ERP LOCATION =======================
    Normalizes country codes and cleans customer location IDs.
*/
        SET @start_time = GETDATE();
        PRINT '----------------------------------------';
        PRINT 'Loading silver.erp_loc_a101';
        PRINT '----------------------------------------';

        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;

        PRINT '>> Inserting Data Info: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT  
            REPLACE(cid, '-', '') AS cid,
            -- Expand country abbreviations and handle blanks
            CASE    
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;

        SET @end_time = GETDATE();
        PRINT '>> Step Duration (silver.erp_loc_a101): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) 
              + ' seconds';
        PRINT '>> ----------------------------------------------------';

/*  
    ======================= ERP CATEGORY =======================
    Loads cleaned category data with no transformation required.
*/
        SET @start_time = GETDATE();
        PRINT '----------------------------------------';
        PRINT 'Loading silver.erp_cat_g1v2';
        PRINT '----------------------------------------';

        PRINT '>> Truncating Table: silver.erp_cat_g1v2';
        TRUNCATE TABLE silver.erp_cat_g1v2;

        PRINT '>> Inserting Data Info: silver.erp_cat_g1v2';
        INSERT INTO silver.erp_cat_g1v2 (
            id,
            cat,
            subcat,
            maintainance
        )
        SELECT 
            id,
            cat,
            subcat,
            maintainance
        FROM bronze.erp_cat_g1v2;

        SET @end_time = GETDATE();
        PRINT '>> Step Duration (silver.erp_cat_g1v2): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(50)) 
              + ' seconds';
        PRINT '>> ----------------------------------------------------';

        /* ======================= TOTAL DURATION ===================== */

        -- End full Silver load timer and print total run duration
        SET @batch_end_time = GETDATE();
        PRINT '========================================================';
        PRINT 'Silver Layer Load Completed';
        PRINT 'Total Silver Load Duration: ' 
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(50)) 
              + ' seconds';
        PRINT '========================================================';

    END TRY
    BEGIN CATCH
        PRINT '========================================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';  -- Print detailed error information if ETL fails
        PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR(50));
        PRINT 'ERROR STATE  : ' + CAST(ERROR_STATE() AS NVARCHAR(50));
        PRINT '========================================================';
    END CATCH
END;

EXEC silver.load_silver;
