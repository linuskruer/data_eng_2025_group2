-- Gold Layer: Analytical Model - Impact of Weather on Listings
-- Based on sql_queries/impact_listings_by_weather.sql
{{ config(
    materialized='table',
    partition_by='toYYYYMM(date)',
    order_by='(date, weather_bucket)',
    docs={'node_color': 'purple'}
) }}

SELECT
    f.date_key AS date,
    p.weather_bucket,
    COUNT(*) AS listings,
    COUNT(DISTINCT f.item_id) AS unique_items,
    AVG(f.price) AS avg_price,
    AVG(f.seller_feedback_percentage) AS avg_seller_feedback,
    -- Additional metrics
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY f.date_key) AS daily_market_share_percent,
    AVG(COUNT(*)) OVER (PARTITION BY p.weather_bucket ORDER BY f.date_key ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_listings_7day
FROM {{ ref('fact_listings') }} f
JOIN {{ ref('dim_product') }} p ON f.product_key = p.product_key
GROUP BY f.date_key, p.weather_bucket
ORDER BY f.date_key, p.weather_bucket