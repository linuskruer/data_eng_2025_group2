-- Incremental Update Queries for ClickHouse Medallion Architecture
-- These queries support incremental data loading and updates

-- Check for new eBay data since last load
SELECT 
    MAX(collection_timestamp) as last_ebay_timestamp,
    COUNT(*) as total_records
FROM bronze.ebay_raw_data;

-- Check for new weather data since last load  
SELECT 
    MAX(time) as last_weather_timestamp,
    COUNT(*) as total_records
FROM bronze.weather_raw_data;

-- Get data quality metrics
SELECT 
    table_name,
    check_type,
    check_result,
    error_count,
    total_count,
    log_timestamp
FROM bronze.data_quality_logs
ORDER BY log_timestamp DESC
LIMIT 10;

-- Sample analytical queries on Bronze data
-- Weather impact on eBay listings
SELECT 
    w.weather_bucket,
    e.weather_category,
    COUNT(*) as listing_count,
    AVG(e.price) as avg_price
FROM (
    SELECT 
        time,
        CASE 
            WHEN temperature_2m_c >= 32 THEN 'Extreme Heat'
            WHEN temperature_2m_c <= -5 THEN 'Extreme Cold'
            WHEN rain_mm > 10 THEN 'Heavy Rain'
            WHEN rain_mm > 0 THEN 'Light Rain'
            ELSE 'Normal'
        END as weather_bucket
    FROM bronze.weather_raw_data
) w
JOIN (
    SELECT 
        collection_timestamp,
        weather_category,
        price
    FROM bronze.ebay_raw_data
) e ON toDate(w.time) = toDate(e.collection_timestamp)
GROUP BY w.weather_bucket, e.weather_category
ORDER BY listing_count DESC;

-- Product type distribution by weather
SELECT 
    weather_category,
    product_type,
    COUNT(*) as listings,
    AVG(price) as avg_price,
    AVG(seller_feedback_percentage) as avg_feedback
FROM bronze.ebay_raw_data
GROUP BY weather_category, product_type
ORDER BY weather_category, listings DESC;
