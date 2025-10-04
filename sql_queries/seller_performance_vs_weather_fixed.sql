-- Fixed: Seller performance tiers vs listing activity under different weather conditions
-- Based on actual data: seller_feedback_score and seller_feedback_percentage fields

WITH seller_tiers AS (
  SELECT
    seller_key,
    CASE
      WHEN seller_feedback_score < 100 THEN 'Low (0-99)'
      WHEN seller_feedback_score < 1000 THEN 'Medium (100-999)'
      WHEN seller_feedback_score < 5000 THEN 'High (1000-4999)'
      ELSE 'Very High (5000+)'
    END AS feedback_score_tier,
    CASE
      WHEN seller_feedback_percentage < 95 THEN 'Poor (<95%)'
      WHEN seller_feedback_percentage < 97 THEN 'Fair (95-97%)'
      WHEN seller_feedback_percentage < 99 THEN 'Good (97-99%)'
      ELSE 'Excellent (99%+)'
    END AS feedback_percentage_tier
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
)
SELECT
  st.feedback_score_tier,
  st.feedback_percentage_tier,
  x.weather_bucket,
  COUNT(*) AS listings,
  COUNT(DISTINCT f.seller_key) AS unique_sellers,
  AVG(f.price) AS avg_price,
  -- Market share by weather condition
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY x.weather_bucket) AS market_share_percent
FROM fact_listings f
JOIN seller_tiers st ON f.seller_key = st.seller_key
JOIN wx_bucket x ON f.weather_category = x.weather_category
JOIN dim_location l ON f.location_key = l.location_key
WHERE l.country_code = 'US'
  AND l.state_code IN (
    'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
  )
GROUP BY st.feedback_score_tier, st.feedback_percentage_tier, x.weather_bucket
ORDER BY x.weather_bucket, listings DESC;
