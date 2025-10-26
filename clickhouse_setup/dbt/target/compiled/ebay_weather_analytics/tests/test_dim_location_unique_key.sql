-- Test: Dimension Location - Unique Location Keys
-- Ensures each location_key appears only once
SELECT location_key
FROM `default`.`dim_location`
GROUP BY location_key
HAVING COUNT(*) > 1