-- Project 3: Limited-access masked views.
-- Mirrors the structure of the full views but redacts sensitive attributes.

CREATE DATABASE IF NOT EXISTS serving_views_masked;

CREATE OR REPLACE VIEW serving_views_masked.vw_listing_weather_masked AS
SELECT
    cityHash64(item_id)                                    AS listing_surrogate,
    event_date,
    product_type,
    weather_code,
    ROUND(price, -1)                                       AS price_bucket,
    ROUND(shipping_cost, -1)                               AS shipping_cost_bucket,
    multiIf(
        seller_feedback_percentage < 90, 'LOW_CONFIDENCE',
        seller_feedback_percentage < 95, 'MODERATE',
        seller_feedback_percentage < 98, 'HIGH',
        'EXCELLENT'
    )                                                       AS seller_feedback_band,
    substring(hex(cityHash64(coalesce(seller_location, 'UNKNOWN'))), 1, 12)
                                                            AS seller_location_masked,
    concat('ZIP_', coalesce(zip_prefix, 'UNK'))             AS zip_prefix_masked,
    free_shipping,
    temperature_2m,
    relative_humidity_2m,
    rain,
    sunshine_duration,
    windspeed_10m
FROM serving_views_full.vw_listing_weather_full;

CREATE OR REPLACE VIEW serving_views_masked.vw_listing_kpis_masked AS
SELECT
    event_date,
    product_type,
    weather_code,
    AVG(price_bucket)                          AS avg_price_bucket,
    AVG(shipping_cost_bucket)                  AS avg_shipping_bucket,
    seller_feedback_band,
    COUNT()                                    AS listings_cnt,
    SUM(CASE WHEN free_shipping THEN 1 ELSE 0 END) AS free_shipping_cnt
FROM serving_views_masked.vw_listing_weather_masked
GROUP BY event_date, product_type, weather_code, seller_feedback_band;

