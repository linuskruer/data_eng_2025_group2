-- Test: Dimension Seller - Unique Seller Keys
-- Ensures each seller_key appears only once
SELECT seller_key
FROM `default`.`dim_seller`
GROUP BY seller_key
HAVING COUNT(*) > 1