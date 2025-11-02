-- Listing quality proxies (title length and buying options) by weather bucket
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
),
buying_option_analysis AS (
  SELECT
    buying_option,
    multiIf(
      buying_option = 'FIXED_PRICE', 'Simple',
      position(buying_option, 'BEST_OFFER') > 0, 'Complex',
      buying_option = 'AUCTION', 'Auction',
      'Other'
    ) AS option_complexity
  FROM (
    SELECT DISTINCT buying_option 
    FROM fact_listings 
    WHERE buying_option IS NOT NULL
      AND buying_option != ''
  ) bo
)
SELECT
  x.weather_bucket,
  boa.option_complexity,
  -- Title length analysis
  avg(f.title_length) AS avg_title_length,
  min(f.title_length) AS min_title_length,
  max(f.title_length) AS max_title_length,
  stddevPop(f.title_length) AS title_length_stddev,
  -- Quality metrics
  count(*) AS listings,
  uniq(f.item_id) AS unique_items,
  -- Quality score (higher is better)
  avg(f.title_length) * count(*) / 1000.0 AS quality_score
FROM fact_listings f
JOIN wx_bucket x ON f.weather_category = x.weather_category
JOIN buying_option_analysis boa ON f.buying_option = boa.buying_option
WHERE f.weather_category IS NOT NULL
  AND f.weather_category != ''
GROUP BY x.weather_bucket, boa.option_complexity
ORDER BY x.weather_bucket, boa.option_complexity;

