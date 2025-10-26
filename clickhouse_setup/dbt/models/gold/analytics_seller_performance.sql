-- Gold Layer: Analytical Model - Seller Performance vs Weather
-- Based on sql_queries/seller_performance_vs_weather.sql
{{ config(
    materialized='table',
    order_by='(feedback_score_tier, weather_bucket)',
    docs={'node_color': 'yellow'}
) }}

SELECT
    s.feedback_score_tier,
    s.feedback_percentage_tier,
    p.weather_bucket,
    COUNT(*) AS listings,
    COUNT(DISTINCT f.item_id) AS unique_items,
    AVG(f.price) AS avg_price,
    AVG(s.seller_feedback_score) AS avg_feedback_score,
    AVG(s.seller_feedback_percentage) AS avg_feedback_percentage,
    -- Market share by weather condition
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY p.weather_bucket) AS market_share_percent,
    -- Additional seller metrics
    AVG(f.title_length) AS avg_title_length,
    SUM(CASE WHEN f.free_shipping THEN 1 ELSE 0 END) AS free_shipping_count,
    SUM(CASE WHEN f.price_quality_flag = 1 THEN 1 ELSE 0 END) AS price_quality_issues
FROM {{ ref('fact_listings') }} f
JOIN {{ ref('dim_seller') }} s ON f.seller_key = s.seller_key
JOIN {{ ref('dim_product') }} p ON f.product_key = p.product_key
GROUP BY s.feedback_score_tier, s.feedback_percentage_tier, p.weather_bucket
ORDER BY p.weather_bucket, listings DESC