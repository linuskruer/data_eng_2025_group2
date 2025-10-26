-- Silver: Clean Weather Data
{{ config(
    materialized='table',
    partition_by='toYYYYMM(time)',
    order_by='(time)'
) }}

SELECT 
    time,
    weather_code_wmo_code,
    temperature_2m_c,
    relative_humidity_2m_percent,
    cloudcover_percent,
    rain_mm,
    sunshine_duration_s,
    windspeed_10m_kmh,
    data_source,
    created_at,
    updated_at,
    -- Add weather buckets
    CASE 
        WHEN temperature_2m_c >= 32 THEN 'Extreme Heat'
        WHEN temperature_2m_c <= -5 THEN 'Extreme Cold'
        WHEN rain_mm > 10 THEN 'Heavy Rain'
        WHEN rain_mm > 0 THEN 'Light Rain'
        ELSE 'Normal'
    END as weather_bucket,
    -- Add data quality flags
    CASE 
        WHEN temperature_2m_c < -50 OR temperature_2m_c > 60 THEN 1 
        ELSE 0 
    END as temperature_quality_flag,
    CASE 
        WHEN rain_mm < 0 THEN 1 
        ELSE 0 
    END as rain_quality_flag
FROM {{ source('bronze', 'weather_raw_data') }}
WHERE time IS NOT NULL