-- Fixed: Impact of weather conditions on daily listings across East Coast
-- Based on actual data analysis: eBay data from 2025-09-30, Weather data from 2025-09-01 to 2025-09-28
-- Issue: No date overlap between eBay and weather data, so we'll use weather_category from eBay data

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
  DATE(collection_ts) AS date,
  x.weather_bucket,
  COUNT(*) AS listings,
  COUNT(DISTINCT item_id) AS unique_items,
  AVG(price) AS avg_price
FROM fact_listings f
JOIN wx_bucket x ON f.weather_category = x.weather_category
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country_code = 'US'
  AND l.state_code IN (
    'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
  )
GROUP BY DATE(collection_ts), x.weather_bucket
ORDER BY date, x.weather_bucket;
