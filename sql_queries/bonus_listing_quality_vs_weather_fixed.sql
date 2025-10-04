-- Fixed: Listing quality proxies (title length and buying options) by weather bucket
-- Based on actual data: title_length integer field and buying_options string field

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
),
buying_option_analysis AS (
  SELECT
    buying_option,
    CASE
      WHEN buying_option = 'FIXED_PRICE' THEN 'Simple'
      WHEN buying_option LIKE '%BEST_OFFER%' THEN 'Complex'
      WHEN buying_option = 'AUCTION' THEN 'Auction'
      ELSE 'Other'
    END AS option_complexity
  FROM (
    SELECT DISTINCT buying_option 
    FROM fact_listings 
    WHERE buying_option IS NOT NULL
  ) bo
)
SELECT
  x.weather_bucket,
  boa.option_complexity,
  -- Title length analysis
  AVG(f.title_length) AS avg_title_length,
  MIN(f.title_length) AS min_title_length,
  MAX(f.title_length) AS max_title_length,
  STDDEV(f.title_length) AS title_length_stddev,
  -- Quality metrics
  COUNT(*) AS listings,
  COUNT(DISTINCT f.item_id) AS unique_items,
  -- Quality score (higher is better)
  AVG(f.title_length) * COUNT(*) / 1000.0 AS quality_score
FROM fact_listings f
JOIN wx_bucket x ON f.weather_category = x.weather_category
JOIN buying_option_analysis boa ON f.buying_option = boa.buying_option
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country_code = 'US'
  AND l.state_code IN (
    'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
  )
GROUP BY x.weather_bucket, boa.option_complexity
ORDER BY x.weather_bucket, boa.option_complexity;
