-- Impact of weather conditions on daily listings across East Coast
-- Converted to ClickHouse syntax
-- Using weather_category from eBay data (fact_listings)

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
  toDate(collection_timestamp) AS date,
  x.weather_bucket,
  count(*) AS listings,
  uniq(item_id) AS unique_items,
  avg(price) AS avg_price
FROM fact_listings f
JOIN wx_bucket x ON f.weather_category = x.weather_category
WHERE f.weather_category IS NOT NULL
  AND f.weather_category != ''
GROUP BY toDate(collection_timestamp), x.weather_bucket
ORDER BY date, x.weather_bucket;

