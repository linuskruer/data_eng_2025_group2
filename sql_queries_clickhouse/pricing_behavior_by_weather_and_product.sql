-- Average price by product type under different weather conditions
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
  avg(f.price) AS avg_price,
  min(f.price) AS min_price,
  max(f.price) AS max_price,
  count(*) AS listings,
  -- Price statistics (ClickHouse uses stddevPop for population stddev)
  stddevPop(f.price) AS price_stddev,
  quantile(0.5)(f.price) AS median_price
FROM fact_listings f
JOIN dim_product p ON f.product_type = p.product_type
JOIN wx_bucket x ON f.weather_category = x.weather_category
WHERE f.weather_category IS NOT NULL
  AND f.weather_category != ''
GROUP BY p.product_type, x.weather_bucket
ORDER BY p.product_type, x.weather_bucket;

