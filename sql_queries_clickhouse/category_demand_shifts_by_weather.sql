-- Product category demand shifts under specific weather conditions
-- Converted to ClickHouse syntax

WITH wx_bucket AS (
  SELECT
    weather_category,
    multiIf(
      weather_category = 'rain_products', 'Precipitation-Heavy',
      weather_category = 'heat_products', 'Extreme Heat',
      weather_category = 'cold_products', 'Extreme Cold',
      'Normal'
    ) AS weather_bucket
  FROM (
    SELECT DISTINCT weather_category 
    FROM fact_listings 
    WHERE weather_category IS NOT NULL
      AND weather_category != ''
  ) w
)
SELECT
  p.product_type,
  x.weather_bucket,
  count(*) AS listings,
  -- Demand metrics (ClickHouse window functions)
  count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY x.weather_bucket) AS percentage_of_weather_bucket,
  count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY p.product_type) AS percentage_of_product_type,
  -- Price metrics
  avg(f.price) AS avg_price,
  min(f.price) AS min_price,
  max(f.price) AS max_price
FROM fact_listings f
JOIN dim_product p ON f.product_type = p.product_type
JOIN wx_bucket x ON f.weather_category = x.weather_category
WHERE f.weather_category IS NOT NULL
  AND f.weather_category != ''
GROUP BY p.product_type, x.weather_bucket
ORDER BY listings DESC, p.product_type, x.weather_bucket;

