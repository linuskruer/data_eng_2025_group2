-- Gold Layer: Dimension Table - Weather
-- Weather dimension with daily aggregations


WITH weather_daily AS (
    SELECT 
        toDate(time) as date,
        AVG(temperature_2m_c) as avg_temperature,
        MIN(temperature_2m_c) as min_temperature,
        MAX(temperature_2m_c) as max_temperature,
        SUM(rain_mm) as total_rain,
        SUM(sunshine_duration_s) as total_sunshine,
        AVG(windspeed_10m_kmh) as avg_wind_speed,
        AVG(relative_humidity_2m_percent) as avg_humidity,
        AVG(cloudcover_percent) as avg_cloudcover,
        MAX(weather_code_wmo_code) as weather_code
    FROM `default`.`silver_weather_data`
    GROUP BY toDate(time)
)

SELECT 
    -- Surrogate Key
    cityHash64(date) as weather_key,
    
    -- Natural Key
    date,
    
    -- Weather Bucket
    CASE 
        WHEN avg_temperature >= 32 THEN 'Extreme Heat'
        WHEN avg_temperature <= -5 THEN 'Extreme Cold'
        WHEN total_rain > 10 THEN 'Heavy Rain'
        WHEN total_rain > 0 THEN 'Light Rain'
        ELSE 'Normal'
    END AS weather_bucket,
    
    -- Temperature attributes
    avg_temperature,
    min_temperature,
    max_temperature,
    CASE 
        WHEN avg_temperature >= 30 THEN 'Hot'
        WHEN avg_temperature >= 20 THEN 'Warm'
        WHEN avg_temperature >= 10 THEN 'Mild'
        WHEN avg_temperature >= 0 THEN 'Cool'
        ELSE 'Cold'
    END AS temperature_category,
    
    -- Precipitation attributes
    total_rain,
    CASE 
        WHEN total_rain > 10 THEN 'Heavy Rain'
        WHEN total_rain > 5 THEN 'Moderate Rain'
        WHEN total_rain > 0 THEN 'Light Rain'
        ELSE 'No Rain'
    END AS rain_category,
    
    -- Other weather attributes
    total_sunshine,
    avg_wind_speed,
    avg_humidity,
    avg_cloudcover,
    weather_code,
    
    -- Seasonal attributes
    CASE 
        WHEN EXTRACT(MONTH FROM date) IN (12, 1, 2) THEN 'Winter'
        WHEN EXTRACT(MONTH FROM date) IN (3, 4, 5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM date) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    
    -- Metadata
    'active' as status,
    now() as created_at,
    now() as updated_at
    
FROM weather_daily