-- Test: Dimension Product - Unique Product Keys
-- Ensures each product_key appears only once
SELECT product_key
FROM {{ ref('dim_product') }}
GROUP BY product_key
HAVING COUNT(*) > 1
