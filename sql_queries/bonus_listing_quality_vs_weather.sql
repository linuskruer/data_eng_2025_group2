-- Listing quality proxies (title length and buying option mix) by weather bucket
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
  x.weather_bucket,
  AVG(f.title_length)::NUMERIC AS avg_title_length,
  b.buying_option,
  COUNT(*) AS listings
FROM fact_listings f
JOIN wx_bucket x ON f.location_key = x.location_key AND f.date_key = x.date_key
JOIN dim_buying_option b ON f.buying_option_key = b.buying_option_key
GROUP BY x.weather_bucket, b.buying_option
ORDER BY x.weather_bucket, b.buying_option;


