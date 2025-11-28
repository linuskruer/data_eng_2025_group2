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
    multiIf(
        weather_code = 0, 'Clear sky',
        weather_code = 1, 'Mainly clear',
        weather_code = 2, 'Partly cloudy',
        weather_code = 3, 'Overcast',
        weather_code >= 45 AND weather_code <= 49, 'Fog',
        weather_code >= 51 AND weather_code <= 55, 'Drizzle',
        weather_code >= 56 AND weather_code <= 57, 'Freezing drizzle',
        weather_code >= 61 AND weather_code <= 65, 'Rain',
        weather_code >= 66 AND weather_code <= 67, 'Freezing rain',
        weather_code >= 71 AND weather_code <= 75, 'Snow',
        weather_code >= 77 AND weather_code <= 82, 'Snow grains/rain showers',
        weather_code = 85 OR weather_code = 86, 'Snow showers',
        weather_code >= 95 AND weather_code <= 99, 'Thunderstorm',
        'Unknown'
    ) AS weather_description,
    AVG(price_bucket)                          AS avg_price_bucket,
    AVG(shipping_cost_bucket)                  AS avg_shipping_bucket,
    seller_feedback_band,
    COUNT()                                    AS listings_cnt,
    SUM(CASE WHEN free_shipping THEN 1 ELSE 0 END) AS free_shipping_cnt
FROM serving_views_masked.vw_listing_weather_masked
GROUP BY event_date, product_type, weather_code, seller_feedback_band;

