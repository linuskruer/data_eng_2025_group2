SELECT DISTINCT
    weather_code,
    CASE 
        WHEN weather_code = 0 THEN 'Clear sky'
        WHEN weather_code = 1 THEN 'Mainly clear'
        WHEN weather_code = 2 THEN 'Partly cloudy'
        WHEN weather_code = 3 THEN 'Overcast'
        WHEN weather_code BETWEEN 45 AND 49 THEN 'Fog'
        WHEN weather_code BETWEEN 51 AND 55 THEN 'Drizzle'
        WHEN weather_code BETWEEN 56 AND 57 THEN 'Freezing drizzle'
        WHEN weather_code BETWEEN 61 AND 65 THEN 'Rain'
        WHEN weather_code BETWEEN 66 AND 67 THEN 'Freezing rain'
        WHEN weather_code BETWEEN 71 AND 75 THEN 'Snow'
        WHEN weather_code BETWEEN 77 AND 82 THEN 'Snow grains/rain showers'
        WHEN weather_code = 85 THEN 'Snow showers'
        WHEN weather_code = 86 THEN 'Snow showers'
        WHEN weather_code BETWEEN 95 AND 99 THEN 'Thunderstorm'
        ELSE 'Unknown'
    END AS weather_description
FROM {{ ref('silver_weather') }}
WHERE weather_code IS NOT NULL





