-- Fixed: Free shipping rate and average shipping cost by weather bucket
-- Based on actual data: free_shipping boolean field and shipping_cost numeric field

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
  x.weather_bucket,
  -- Free shipping analysis
  AVG(CASE WHEN f.free_shipping THEN 1.0 ELSE 0.0 END) AS free_shipping_rate,
  COUNT(CASE WHEN f.free_shipping THEN 1 END) AS free_shipping_count,
  COUNT(CASE WHEN NOT f.free_shipping THEN 1 END) AS paid_shipping_count,
  -- Shipping cost analysis
  AVG(CASE WHEN NOT f.free_shipping THEN f.shipping_cost END) AS avg_paid_shipping_cost,
  MIN(CASE WHEN NOT f.free_shipping THEN f.shipping_cost END) AS min_shipping_cost,
  MAX(CASE WHEN NOT f.free_shipping THEN f.shipping_cost END) AS max_shipping_cost,
  -- Overall statistics
  COUNT(*) AS total_listings
FROM fact_listings f
JOIN wx_bucket x ON f.weather_category = x.weather_category
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country_code = 'US'
  AND l.state_code IN (
    'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
  )
GROUP BY x.weather_bucket
ORDER BY x.weather_bucket;
