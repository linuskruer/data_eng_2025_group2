-- Project 3: ClickHouse S3 Table Engine for Direct Iceberg Querying

CREATE DATABASE IF NOT EXISTS iceberg_readonly;

CREATE TABLE IF NOT EXISTS iceberg_readonly.weather_hourly_s3
(
    event_time DateTime,
    event_date Date,
    city String,
    location_id Int32,
    postal_code String,
    postal_prefix String,
    weather_code Int32,
    temperature_2m Float64,
    relative_humidity_2m Float64,
    cloudcover Float64,
    rain Float64,
    sunshine_duration Float64,
    windspeed_10m Float64,
    ingested_at DateTime
)
ENGINE = S3(
    'http://project3-minio:9000/iceberg-bronze/warehouse/project3_bronze.db/weather_hourly/data/*.parquet',
    'project3admin',
    'project3admin123',
    'Parquet'
)
-- Note: S3 engine tables are read-only by default

-- Grant read access to both analyst roles (run separately after roles are created)
-- Note: Run these commands separately if roles exist:
--   GRANT SELECT ON iceberg_readonly.* TO analyst_full;
--   GRANT SELECT ON iceberg_readonly.* TO analyst_limited;

-- Example queries to verify the table works:
-- SELECT COUNT(*) FROM iceberg_readonly.weather_hourly_s3;
-- SELECT * FROM iceberg_readonly.weather_hourly_s3 ORDER BY event_time DESC LIMIT 10;
-- SELECT city, AVG(temperature_2m) as avg_temp FROM iceberg_readonly.weather_hourly_s3 GROUP BY city;

