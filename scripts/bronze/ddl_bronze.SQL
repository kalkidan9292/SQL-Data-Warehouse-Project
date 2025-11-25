/* ====================================================================================================
   DDL Script: Create Bronze Tables
   ----------------------------------------------------------------------------------------------------
   Purpose:
     This script creates all tables required in the 'bronze' schema for the Data Warehouse project.
     It safely drops existing tables (if they already exist) and recreates them with the defined schema.

   Usage:
     Run this script before loading data into the Bronze Layer. This ensures all structures are fresh,
     consistent, and aligned with the project’s data modeling specifications.

   Notes:
     - Follows Medallion Architecture (Bronze → Silver → Gold).
     - Bronze layer stores raw, untransformed data.
     - All tables follow naming standards: source_system_tableName.

   Author: Kalkidan Bekele Argaw
   ==================================================================================================== */


--------------------------------------------------
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE
);

--------------------------------------------------
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

--------------------------------------------------
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_ord_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,      -- change to DATE/DATETIME if source is date
    sls_ship_dt  INT,      -- same note
    sls_due_dt   INT,      -- same note
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

--------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);

--------------------------------------------------
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);

--------------------------------------------------
IF OBJECT_ID('bronze.erp_cat_g1v2','U') IS NOT NULL
    DROP TABLE bronze.erp_cat_g1v2;

CREATE TABLE bronze.erp_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintainance NVARCHAR(50)
);
--------------------------------------------------
IF OBJECT_ID('bronze.crm_cust_info','U') IS NOT NULL
    DROP TABLE bronze.crm_cust_info;

CREATE TABLE bronze.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_material_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE
);

--------------------------------------------------
IF OBJECT_ID('bronze.crm_prd_info','U') IS NOT NULL
    DROP TABLE bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      NVARCHAR(50),
    prd_nm       NVARCHAR(50),
    prd_cost     INT,
    prd_line     NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

--------------------------------------------------
IF OBJECT_ID('bronze.crm_sales_details','U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  NVARCHAR(50),
    sls_ord_key  NVARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,      -- change to DATE/DATETIME if source is date
    sls_ship_dt  INT,      -- same note
    sls_due_dt   INT,      -- same note
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

--------------------------------------------------
IF OBJECT_ID('bronze.erp_loc_a101','U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;

CREATE TABLE bronze.erp_loc_a101 (
    cid   NVARCHAR(50),
    cntry NVARCHAR(50)
);

--------------------------------------------------
IF OBJECT_ID('bronze.erp_cust_az12','U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;

CREATE TABLE bronze.erp_cust_az12 (
    cid   NVARCHAR(50),
    bdate DATE,
    gen   NVARCHAR(50)
);

--------------------------------------------------
IF OBJECT_ID('bronze.erp_cat_g1v2','U') IS NOT NULL
    DROP TABLE bronze.erp_cat_g1v2;

CREATE TABLE bronze.erp_cat_g1v2 (
    id           NVARCHAR(50),
    cat          NVARCHAR(50),
    subcat       NVARCHAR(50),
    maintainance NVARCHAR(50)
);
