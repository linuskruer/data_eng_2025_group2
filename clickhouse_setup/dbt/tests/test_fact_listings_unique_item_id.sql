-- Test: Fact Listings - Unique Item IDs
-- Ensures each item_id appears only once in the fact table
SELECT item_id
FROM {{ ref('fact_listings') }}
GROUP BY item_id
HAVING COUNT(*) > 1
