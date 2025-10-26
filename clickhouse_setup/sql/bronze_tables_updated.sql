-- Updated Bronze Layer Tables for Medallion Architecture
-- These tables store raw data as ingested from APIs

-- Bronze: Raw eBay Data
CREATE TABLE IF NOT EXISTS bronze.ebay_raw_data (
    collection_timestamp DateTime64(6),
    timezone String,
    weather_category String,
    product_type String,
    price Float64,
    currency String,
    seller_feedback_percentage Float64,
    seller_feedback_score UInt32,
    item_location String,
    seller_location String,
    shipping_cost Float64,
    free_shipping Bool,
    condition String,
    buying_options String,
    title_length UInt16,
    item_id String,
    marketplace_id String,
    -- Metadata fields
    execution_date Date,
    data_source String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(collection_timestamp)
ORDER BY (collection_timestamp, item_id)
SETTINGS index_granularity = 8192;

-- Bronze: Raw Weather Data (Updated column names)
CREATE TABLE IF NOT EXISTS bronze.weather_raw_data (
    time DateTime,
    weather_code_wmo_code UInt8,
    temperature_2m_c Float64,
    relative_humidity_2m_percent Float64,
    cloudcover_percent Float64,
    rain_mm Float64,
    sunshine_duration_s Float64,
    windspeed_10m_kmh Float64,
    -- Metadata fields
    data_source String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(time)
ORDER BY (time)
SETTINGS index_granularity = 8192;

-- Bronze: Data Quality Logs
CREATE TABLE IF NOT EXISTS bronze.data_quality_logs (
    log_timestamp DateTime,
    table_name String,
    check_type String,
    check_result String,
    error_count UInt32,
    total_count UInt32,
    details String,
    execution_id String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(log_timestamp)
ORDER BY (log_timestamp, table_name, check_type)
SETTINGS index_granularity = 8192;
