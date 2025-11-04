-- Free shipping rate and average shipping cost by weather bucket
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
  x.weather_bucket,
  -- Free shipping analysis (ClickHouse: use if() for CASE)
  avg(if(f.free_shipping, 1.0, 0.0)) AS free_shipping_rate,
  countIf(f.free_shipping) AS free_shipping_count,
  countIf(NOT f.free_shipping) AS paid_shipping_count,
  -- Shipping cost analysis
  avgIf(f.shipping_cost, NOT f.free_shipping) AS avg_paid_shipping_cost,
  minIf(f.shipping_cost, NOT f.free_shipping) AS min_shipping_cost,
  maxIf(f.shipping_cost, NOT f.free_shipping) AS max_shipping_cost,
  -- Overall statistics
  count(*) AS total_listings
FROM fact_listings f
JOIN wx_bucket x ON f.weather_category = x.weather_category
WHERE f.weather_category IS NOT NULL
  AND f.weather_category != ''
GROUP BY x.weather_bucket
ORDER BY x.weather_bucket;

