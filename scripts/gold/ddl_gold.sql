/*
============================================================
Gold Layer – View Definitions
Purpose:
    Business-ready views for analytics and reporting, built
    on top of the Silver layer and Gold dimensions.
============================================================
*/


/*===========================================================
 View: gold.dim_customers
-------------------------------------------------------------
 Description:
    Customer dimension view that combines CRM customer data
    with ERP demographics and location to create a single
    conformed customer profile.
 Grain:
    1 row per customer_id.
 Sources:
    - silver.crm_cust_info
    - silver.erp_cust_az12
    - silver.erp_loc_a101
===========================================================*/
CREATE VIEW gold.dim_customers AS
SELECT 
       ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
       ci.cst_id AS customer_id, 
       ci.cst_key AS customer_name,
       ci.cst_firstname AS firstname,
       ci.cst_lastname AS last_name,
       la.cntry AS country,
       ci.cst_material_status AS martial_status,
       CASE
            WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr -- CRM is the master for the gender info
            ELSE COALESCE(ca.gen, 'Unknown')
        END AS gender,
       ca.bdate AS birthdate,
       ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la ON ci.cst_key = la.cid;
GO


/*===========================================================
 View: gold.dim_products
-------------------------------------------------------------
 Description:
    Product dimension view that enriches CRM product records
    with ERP category attributes and filters to the current,
    active products only.
 Grain:
    1 row per current product (prd_key with prd_end_dt IS NULL).
 Sources:
    - silver.crm_prd_info
    - silver.erp_cat_g1v2
===========================================================*/
CREATE VIEW gold.dim_products AS
SELECT 
        ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
        pn.prd_id AS product_id,    
        pn.prd_key AS product_number,
        pn.prd_nm AS product_name,
        pn.cat_id AS category_id,    
        pc.cat AS category,
        pc.subcat AS sub_category,        
        pc.maintainance,
        pn.prd_cost AS cost,
        pn.prd_line AS product_line,
        pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_cat_g1v2 pc ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL; -- Filter out all historical data
GO


/*===========================================================
 View: gold.fact_sales
-------------------------------------------------------------
 Description:
    Sales fact view that joins cleansed sales transactions
    from Silver with Gold product and customer dimensions to
    support reporting and BI analytics.
 Grain:
    1 row per sales order line (sls_ord_num).
 Sources:
    - silver.crm_sales_details
    - gold.dim_products
    - gold.dim_customers
===========================================================*/
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products  pr ON sd.sls_ord_key  = pr.product_number
LEFT JOIN gold.dim_customers cu ON sd.sls_cust_id  = cu.customer_id;
GO
