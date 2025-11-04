-- Price and listing availability variation by zip prefix and weather bucket
-- Converted to ClickHouse syntax - using zip_prefix from dim_location

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
),
zip_weather_stats AS (
  SELECT
    l.zip_prefix,
    x.weather_bucket,
    avg(f.price) AS avg_price,
    count(*) AS listings,
    stddevPop(f.price) AS price_stddev
  FROM fact_listings f
  JOIN wx_bucket x ON f.weather_category = x.weather_category
  LEFT JOIN dim_location l ON f.location_name = l.location_name
  WHERE f.weather_category IS NOT NULL
    AND f.weather_category != ''
    AND (l.zip_prefix IS NOT NULL OR position(f.location_name, 'postalCode') > 0)
  GROUP BY l.zip_prefix, x.weather_bucket
)
SELECT
  zip_prefix,
  weather_bucket,
  avg_price,
  listings,
  price_stddev,
  -- Calculate variation coefficient
  price_stddev / avg_price AS price_variation_coefficient,
  -- Calculate price variability across weather conditions for this zip
  stddevPop(avg_price) OVER (PARTITION BY zip_prefix) AS cross_weather_price_variability,
  -- Market share within weather bucket
  listings * 100.0 / sum(listings) OVER (PARTITION BY weather_bucket) AS market_share_percent
FROM zip_weather_stats
WHERE listings >= 2  -- Only include zip codes with multiple listings
ORDER BY cross_weather_price_variability DESC NULLS LAST, listings DESC;

