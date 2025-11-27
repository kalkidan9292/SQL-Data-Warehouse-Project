/*
============================================================
Silver Layer – Data Quality Checks
Purpose:
    Ad-hoc queries to validate data quality in Silver tables
    (and a few supporting checks on Bronze where needed).
    Expectation for most checks: **no rows returned**.
============================================================
*/

------------------------------------------------------------
-- 1. CRM CUSTOMER – silver.crm_cust_info
------------------------------------------------------------

-- 1.1 Check for NULLs or duplicate customer IDs
-- Expectation: no rows
SELECT 
    cst_id,
    COUNT(*) AS cnt
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1
   OR cst_id IS NULL;


-- 1.2 Check for unwanted spaces in first name
-- Expectation: no rows
SELECT cst_id, cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname <> TRIM(cst_firstname);


-- 1.3 Check gender standardization
-- Expectation: only 'Female', 'Male', 'Unknown'
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info
ORDER BY cst_gndr;



------------------------------------------------------------
-- 2. CRM PRODUCT – silver.crm_prd_info
------------------------------------------------------------

-- 2.1 Check for NULLs or duplicate product IDs
-- Expectation: no rows
SELECT 
    prd_id,
    COUNT(*) AS cnt
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1
   OR prd_id IS NULL;


-- 2.2 Check for unwanted spaces in product name
-- Expectation: no rows
SELECT prd_id, prd_nm
FROM silver.crm_prd_info
WHERE prd_nm <> TRIM(prd_nm);


-- 2.3 Check for NULL or negative product cost
-- Expectation: no rows
SELECT prd_id, prd_cost
FROM silver.crm_prd_info
WHERE prd_cost IS NULL
   OR prd_cost < 0;


-- 2.4 Check standardized product lines
-- Expectation: values like 'Mountain', 'Road', 'Other Sales', 'Touring', 'Unknown'
SELECT DISTINCT prd_line
FROM silver.crm_prd_info
ORDER BY prd_line;


-- 2.5 Check for invalid date ranges (end < start)
-- Expectation: no rows
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt IS NOT NULL
  AND prd_end_dt < prd_start_dt;



------------------------------------------------------------
-- 3. CRM SALES DETAILS – silver.crm_sales_details
------------------------------------------------------------

-- 3.1 Check for invalid order dates in Bronze (source)
-- Expectation: no rows
SELECT DISTINCT NULLIF(sls_order_dt, 0) AS sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
   OR LEN(sls_order_dt) <> 8 
   OR sls_order_dt > 20500101 
   OR sls_order_dt < 19000101;


-- 3.2 Check for invalid date order in Silver
-- Expectation: no rows
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
   OR sls_order_dt > sls_due_dt;


-- 3.3 Data consistency between Sales, Quantity, and Price
-- Expectation: no rows
-- Rule: sls_sales = sls_quantity * sls_price and values > 0
SELECT DISTINCT 
       sls_sales,
       sls_quantity,
       sls_price
FROM silver.crm_sales_details
WHERE sls_sales IS NULL 
   OR sls_quantity IS NULL
   OR sls_price IS NULL
   OR sls_sales <= 0
   OR sls_quantity <= 0
   OR sls_price <= 0
   OR sls_sales <> sls_quantity * sls_price
ORDER BY sls_sales, sls_quantity, sls_price;




------------------------------------------------------------
-- 4. ERP CUSTOMER – silver.erp_cust_az12
------------------------------------------------------------

-- 4.1 Identify out-of-range birthdates
-- Expectation: no rows
SELECT DISTINCT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01'
   OR bdate > GETDATE();


-- 4.2 Check standardized gender values
-- Expectation: only 'Female', 'Male', 'Unknown'
SELECT DISTINCT 
    gen
FROM silver.erp_cust_az12
ORDER BY gen;



------------------------------------------------------------
-- 5. ERP LOCATION – silver.erp_loc_a101
------------------------------------------------------------

-- 5.1 Preview cleaned location transformation (for troubleshooting)
SELECT  
    REPLACE(cid, '-', '') AS cid_clean,
    CASE    
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
        ELSE TRIM(cntry)
    END AS cntry_clean
FROM silver.erp_loc_a101;


-- 5.2 Check distinct country values after standardization
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;




------------------------------------------------------------
-- 6. ERP CATEGORY – bronze.erp_cat_g1v2 (source for Silver)
------------------------------------------------------------

-- 6.1 Check for unwanted spaces in category fields
-- Expectation: no rows
SELECT *
FROM bronze.erp_cat_g1v2
WHERE cat          <> TRIM(cat)
   OR subcat       <> TRIM(subcat)
   OR maintainance <> TRIM(maintainance);


-- 6.2 Distinct category values (for profiling)
SELECT DISTINCT cat
FROM bronze.erp_cat_g1v2
ORDER BY cat;

SELECT DISTINCT subcat
FROM bronze.erp_cat_g1v2
ORDER BY subcat;

SELECT DISTINCT maintainance
FROM bronze.erp_cat_g1v2
ORDER BY maintainance;
