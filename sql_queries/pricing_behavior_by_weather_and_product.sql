-- Fixed: Average price by product type under different weather conditions
-- Based on actual data: 9 product types with weather_category from eBay data

WITH wx_bucket AS (
  SELECT
    weather_category,
    CASE
      WHEN weather_category = 'rain_products' THEN 'Precipitation-Heavy'
      WHEN weather_category = 'heat_products' THEN 'Extreme Heat'
      WHEN weather_category = 'cold_products' THEN 'Extreme Cold'
      ELSE 'Normal'
    END AS weather_bucket
  FROM (
    SELECT DISTINCT weather_category 
    FROM fact_listings 
    WHERE weather_category IS NOT NULL
  ) w
)
SELECT
  p.product_type,
  x.weather_bucket,
  AVG(f.price) AS avg_price,
  MIN(f.price) AS min_price,
  MAX(f.price) AS max_price,
  COUNT(*) AS listings,
  -- Price statistics
  STDDEV(f.price) AS price_stddev,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.price) AS median_price
FROM fact_listings f
JOIN dim_product p ON f.product_key = p.product_key
JOIN wx_bucket x ON f.weather_category = x.weather_category
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country_code = 'US'
  AND l.state_code IN (
    'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
  )
GROUP BY p.product_type, x.weather_bucket
ORDER BY p.product_type, x.weather_bucket;
