-- Fixed: Price and listing availability variation by zip prefix and weather bucket
-- Based on actual data: zip codes extracted from item_location field

WITH zip_extraction AS (
  SELECT
    listing_key,
    item_id,
    price,
    weather_category,
    -- Extract zip prefix from item_location JSON-like string
    CASE
      WHEN item_location LIKE '%331**%' THEN '331'
      WHEN item_location LIKE '%112**%' THEN '112'
      WHEN item_location LIKE '%113**%' THEN '113'
      WHEN item_location LIKE '%114**%' THEN '114'
      WHEN item_location LIKE '%103**%' THEN '103'
      WHEN item_location LIKE '%100**%' THEN '100'
      WHEN item_location LIKE '%111**%' THEN '111'
      WHEN item_location LIKE '%191**%' THEN '191'
      WHEN item_location LIKE '%021**%' THEN '021'
      WHEN item_location LIKE '%022**%' THEN '022'
      WHEN item_location LIKE '%104**%' THEN '104'
      ELSE 'Unknown'
    END AS zip_prefix
  FROM fact_listings
),
wx_bucket AS (
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
),
zip_weather_stats AS (
  SELECT
    ze.zip_prefix,
    x.weather_bucket,
    AVG(ze.price) AS avg_price,
    COUNT(*) AS listings,
    STDDEV(ze.price) AS price_stddev
  FROM zip_extraction ze
  JOIN wx_bucket x ON ze.weather_category = x.weather_category
  WHERE ze.zip_prefix != 'Unknown'
  GROUP BY ze.zip_prefix, x.weather_bucket
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
  STDDEV(avg_price) OVER (PARTITION BY zip_prefix) AS cross_weather_price_variability,
  -- Market share within weather bucket
  listings * 100.0 / SUM(listings) OVER (PARTITION BY weather_bucket) AS market_share_percent
FROM zip_weather_stats
WHERE listings >= 2  -- Only include zip codes with multiple listings
ORDER BY cross_weather_price_variability DESC NULLS LAST, listings DESC;
