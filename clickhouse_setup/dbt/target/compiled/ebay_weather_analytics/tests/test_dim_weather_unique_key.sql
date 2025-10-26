-- Test: Dimension Weather - Unique Weather Keys
-- Ensures each weather_key appears only once
SELECT weather_key
FROM `default`.`dim_weather`
GROUP BY weather_key
HAVING COUNT(*) > 1