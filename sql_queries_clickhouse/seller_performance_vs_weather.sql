-- Seller performance tiers vs listing activity under different weather conditions
-- Converted to ClickHouse syntax

WITH seller_tiers AS (
  SELECT
    seller_feedback_score,
    seller_feedback_percentage,
    multiIf(
      seller_feedback_score < 100, 'Low (0-99)',
      seller_feedback_score < 1000, 'Medium (100-999)',
      seller_feedback_score < 5000, 'High (1000-4999)',
      'Very High (5000+)'
    ) AS feedback_score_tier,
    multiIf(
      seller_feedback_percentage < 95, 'Poor (<95%)',
      seller_feedback_percentage < 97, 'Fair (95-97%)',
      seller_feedback_percentage < 99, 'Good (97-99%)',
      'Excellent (99%+)'
    ) AS feedback_percentage_tier
  FROM fact_listings
  WHERE seller_feedback_score IS NOT NULL
    AND seller_feedback_percentage IS NOT NULL
),
wx_bucket AS (
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
  st.feedback_score_tier,
  st.feedback_percentage_tier,
  x.weather_bucket,
  count(*) AS listings,
  uniq(f.item_id) AS unique_sellers,  -- Using item_id as proxy since we don't have seller_key
  avg(f.price) AS avg_price,
  -- Market share by weather condition (ClickHouse window functions)
  count(*) * 100.0 / sum(count(*)) OVER (PARTITION BY x.weather_bucket) AS market_share_percent
FROM fact_listings f
JOIN seller_tiers st ON f.seller_feedback_score = st.seller_feedback_score 
  AND f.seller_feedback_percentage = st.seller_feedback_percentage
JOIN wx_bucket x ON f.weather_category = x.weather_category
WHERE f.weather_category IS NOT NULL
  AND f.weather_category != ''
GROUP BY st.feedback_score_tier, st.feedback_percentage_tier, x.weather_bucket
ORDER BY x.weather_bucket, listings DESC;

