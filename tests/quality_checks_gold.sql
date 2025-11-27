/*
===============================================================================
Gold Layer – Quality Checks
===============================================================================
Purpose:
    Validate the integrity, consistency, and accuracy of the Gold Layer.

    This script checks:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Connectivity of the data model for analytics.

Usage:
    Run after loading Gold.
    Any returned rows indicate data issues that should be investigated.
===============================================================================
*/

-- ====================================================================
-- 1. Check 'gold.dim_customers'
-- ====================================================================
-- Uniqueness of customer_key (surrogate key)
-- Expectation: no rows
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- 2. Check 'gold.dim_products'
-- ====================================================================
-- Uniqueness of product_key (surrogate key)
-- Expectation: no rows
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


-- ====================================================================
-- 3. Check 'gold.fact_sales'
-- ====================================================================
-- Referential integrity between fact_sales and dimensions
-- Expectation: no rows (all product_key and customer_key should resolve)
SELECT 
    f.*,
    c.customer_key,
    p.product_key
FROM gold.fact_sales      AS f
LEFT JOIN gold.dim_customers AS c ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products  AS p ON p.product_key = f.product_key
WHERE p.product_key IS NULL 
   OR c.customer_key IS NULL;
