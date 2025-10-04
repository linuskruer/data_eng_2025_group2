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
), base AS (
  SELECT
    l.zip_prefix,
    x.weather_bucket,
    AVG(f.price) AS avg_price,
    COUNT(*) AS listings
  FROM fact_listings f
  JOIN dim_location l ON f.location_key = l.location_key
  JOIN wx_bucket x ON f.location_key = x.location_key AND f.date_key = x.date_key
  WHERE l.zip_prefix IS NOT NULL AND l.zip_prefix <> ''
  GROUP BY l.zip_prefix, x.weather_bucket
)
SELECT
  zip_prefix,
  weather_bucket,
  avg_price,
  listings,
  STDDEV_SAMP(avg_price) OVER (PARTITION BY zip_prefix) AS price_variability_across_weather
FROM base
ORDER BY price_variability_across_weather DESC NULLS LAST, listings DESC;


