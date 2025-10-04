-- Fixed: Product category demand shifts under specific weather conditions
-- Based on actual data: 9 product types with clear weather-related categorization

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
  COUNT(*) AS listings,
  -- Demand metrics
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY x.weather_bucket) AS percentage_of_weather_bucket,
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY p.product_type) AS percentage_of_product_type,
  -- Price metrics
  AVG(f.price) AS avg_price,
  MIN(f.price) AS min_price,
  MAX(f.price) AS max_price
FROM fact_listings f
JOIN dim_product p ON f.product_key = p.product_key
JOIN wx_bucket x ON f.weather_category = x.weather_category
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country_code = 'US'
  AND l.state_code IN (
    'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
  )
GROUP BY p.product_type, x.weather_bucket
ORDER BY listings DESC, p.product_type, x.weather_bucket;
