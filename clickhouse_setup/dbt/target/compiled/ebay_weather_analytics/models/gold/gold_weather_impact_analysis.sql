-- Gold: Weather Impact Analysis


WITH weather_daily AS (
    SELECT 
        toDate(time) as date,
        AVG(temperature_2m_c) as avg_temperature,
        SUM(rain_mm) as total_rain,
        SUM(sunshine_duration_s) as total_sunshine,
        AVG(windspeed_10m_kmh) as avg_wind_speed,
        MAX(CASE 
            WHEN temperature_2m_c >= 32 THEN 'Extreme Heat'
            WHEN temperature_2m_c <= -5 THEN 'Extreme Cold'
            WHEN rain_mm > 10 THEN 'Heavy Rain'
            WHEN rain_mm > 0 THEN 'Light Rain'
            ELSE 'Normal'
        END) as weather_bucket
    FROM `default`.`silver_weather_data`
    GROUP BY toDate(time)
),
ebay_daily AS (
    SELECT 
        toDate(collection_timestamp) as date,
        weather_category,
        product_type,
        COUNT(*) as listings_count,
        AVG(price) as avg_price,
        AVG(seller_feedback_percentage) as avg_seller_feedback
    FROM `default`.`silver_ebay_data`
    GROUP BY 
        toDate(collection_timestamp),
        weather_category,
        product_type
)
SELECT 
    e.date,
    w.weather_bucket,
    w.avg_temperature,
    w.total_rain,
    w.total_sunshine,
    w.avg_wind_speed,
    e.weather_category,
    e.product_type,
    e.listings_count,
    e.avg_price,
    e.avg_seller_feedback,
    -- Calculate weather impact metrics
    CASE 
        WHEN w.weather_bucket = 'Extreme Heat' AND e.weather_category = 'heat_products' THEN 1
        WHEN w.weather_bucket = 'Extreme Cold' AND e.weather_category = 'cold_products' THEN 1
        WHEN w.weather_bucket = 'Heavy Rain' AND e.weather_category = 'rain_products' THEN 1
        ELSE 0
    END as weather_demand_alignment
FROM ebay_daily e
LEFT JOIN weather_daily w ON e.date = w.date