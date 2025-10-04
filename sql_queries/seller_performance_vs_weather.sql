-- Seller performance tiers vs listing activity under bad weather
WITH bad_weather AS (
  SELECT w.location_key, w.date AS date_key
  FROM dim_weather w
  JOIN dim_location l ON w.location_key = l.location_key
  WHERE l.country_code = 'US'
    AND l.state_code IN (
      'ME','NH','VT','MA','RI','CT','NY','NJ','PA','DE','MD','DC','VA','NC','SC','GA','FL'
    )
    AND (
      w.rain > 10 OR w.temperature_2m >= 32 OR w.temperature_2m <= -5
    )
)
SELECT
  s.feedback_score_bin,
  s.feedback_percentage_bin,
  COUNT(*) AS listings
FROM fact_listings f
JOIN dim_seller s ON f.seller_key = s.seller_key
JOIN bad_weather bw ON f.location_key = bw.location_key AND f.date_key = bw.date_key
GROUP BY s.feedback_score_bin, s.feedback_percentage_bin
ORDER BY listings DESC;


