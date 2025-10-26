-- Test: Fact Listings - Not Null Critical Fields
-- Ensures critical fields are not null
SELECT *
FROM `default`.`fact_listings`
WHERE item_id IS NULL
   OR collection_timestamp IS NULL
   OR price IS NULL
   OR product_type IS NULL
   OR weather_category IS NULL