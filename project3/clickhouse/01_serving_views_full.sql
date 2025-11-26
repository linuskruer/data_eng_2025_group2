-- Project 3: Full-access analytical views in ClickHouse
-- Run after dbt has materialized gold tables in the `default` database.

CREATE DATABASE IF NOT EXISTS serving_views_full;

CREATE OR REPLACE VIEW serving_views_full.vw_listing_weather_full AS
SELECT
    f.item_id,
    toDate(f.collection_timestamp)            AS event_date,
    f.collection_timestamp,
    f.product_type,
    f.location_name,
    f.price,
    f.shipping_cost,
    f.free_shipping,
    f.seller_location,
    f.seller_feedback_percentage,
    f.weather_category,
    loc.state_code,
    loc.zip_prefix,
    w.weather_code,
    w.temperature_2m,
    w.relative_humidity_2m,
    w.rain,
    w.sunshine_duration,
    w.windspeed_10m
FROM default.fact_listings AS f
LEFT JOIN default.dim_location AS loc
    ON f.location_name = loc.location_name
LEFT JOIN default.fact_weather AS w
    ON loc.zip_prefix = w.postal_prefix
   AND toDate(f.collection_timestamp) = toDate(w.time);

CREATE OR REPLACE VIEW serving_views_full.vw_listing_kpis_full AS
SELECT
    event_date,
    product_type,
    weather_code,
    AVG(price)                                  AS avg_listing_price,
    AVG(shipping_cost)                          AS avg_shipping_cost,
    AVG(seller_feedback_percentage)             AS avg_seller_feedback_pct,
    COUNT()                                     AS listings_cnt,
    SUM(CASE WHEN free_shipping THEN 1 ELSE 0 END) AS free_shipping_cnt
FROM serving_views_full.vw_listing_weather_full
GROUP BY event_date, product_type, weather_code;

