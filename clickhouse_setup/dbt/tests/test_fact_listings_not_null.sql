-- Test: Fact Listings - Not Null Critical Fields
-- Ensures critical fields are not null
SELECT *
FROM {{ ref('fact_listings') }}
WHERE item_id IS NULL
   OR collection_timestamp IS NULL
   OR price IS NULL
   OR product_key IS NULL
   OR location_key IS NULL
   OR seller_key IS NULL