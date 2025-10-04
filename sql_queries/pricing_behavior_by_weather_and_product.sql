-- Average price by product type under extreme weather conditions
WITH wx_bucket AS (
  SELECT
    w.location_key,
    w.date AS date_key,
    CASE
      WHEN w.rain > 10 THEN 'Precipitation-Heavy'
      WHEN w.temperature_2m >= 32 THEN 'Extreme Heat'
      WHEN w.temperature_2m <= -5 THEN 'Extreme Cold'
      ELSE 'Normal'
    END AS weather_bucket
  FROM dim_weather w
  JOIN dim_location l ON w.location_key = l.location_key
  WHERE l.country_code = 'US'
    AND l.state_code IN (
      'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
    )
)
SELECT
  p.product_type,
  x.weather_bucket,
  AVG(f.price) AS avg_price,
  COUNT(*) AS listings
FROM fact_listings f
JOIN dim_product p ON f.product_key = p.product_key
JOIN wx_bucket x ON f.location_key = x.location_key AND f.date_key = x.date_key
GROUP BY p.product_type, x.weather_bucket
ORDER BY p.product_type, x.weather_bucket;


