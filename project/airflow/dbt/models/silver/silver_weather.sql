{{ config(
    materialized='incremental',
    unique_key='time_city'
) }}

WITH cleaned AS (
    SELECT
        time,
        weather_code,
        temperature_2m,
        relative_humidity_2m,
        cloudcover,
        rain,
        sunshine_duration,
        windspeed_10m,
        city,
        postal_prefix,
        file_version,
        ingestion_timestamp,
        CONCAT(time, '_', city) AS time_city  -- for incremental key
    FROM {{ source('bronze', 'bronze_weather') }}
    WHERE time IS NOT NULL
)
SELECT *
FROM cleaned