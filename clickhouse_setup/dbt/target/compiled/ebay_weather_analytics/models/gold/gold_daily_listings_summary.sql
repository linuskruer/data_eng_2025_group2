-- Gold Layer Models for dbt
-- These models create business-ready analytical tables

-- Gold: Daily eBay Listings Summary


SELECT 
    toDate(collection_timestamp) as date,
    weather_category,
    product_type,
    postal_code,
    country,
    COUNT(*) as total_listings,
    COUNT(DISTINCT item_id) as unique_items,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    stddevPop(price) as price_stddev,
    AVG(seller_feedback_percentage) as avg_seller_feedback,
    SUM(CASE WHEN free_shipping THEN 1 ELSE 0 END) as free_shipping_count,
    SUM(CASE WHEN free_shipping THEN 0 ELSE 1 END) as paid_shipping_count,
    AVG(CASE WHEN NOT free_shipping THEN shipping_cost END) as avg_paid_shipping_cost,
    AVG(title_length) as avg_title_length,
    SUM(price_quality_flag) as price_quality_issues,
    SUM(feedback_quality_flag) as feedback_quality_issues
FROM `default`.`silver_ebay_data`
GROUP BY 
    toDate(collection_timestamp),
    weather_category,
    product_type,
    postal_code,
    country