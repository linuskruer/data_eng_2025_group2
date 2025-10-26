
  
    
    
    
        
         


        insert into `default`.`analytics_pricing_behavior`
        ("product_type", "weather_bucket", "avg_price", "min_price", "max_price", "listings", "price_stddev", "median_price", "q1_price", "q3_price", "price_range", "price_coefficient_variation", "avg_seller_feedback", "free_shipping_count", "paid_shipping_count")-- Gold Layer: Analytical Model - Pricing Behavior by Weather and Product
-- Based on sql_queries/pricing_behavior_by_weather_and_product.sql


SELECT
    p.product_type,
    p.weather_bucket,
    AVG(f.price) AS avg_price,
    MIN(f.price) AS min_price,
    MAX(f.price) AS max_price,
    COUNT(*) AS listings,
    stddevPop(f.price) AS price_stddev,
    quantile(0.5)(f.price) AS median_price,
    quantile(0.25)(f.price) AS q1_price,
    quantile(0.75)(f.price) AS q3_price,
    -- Price range analysis
    MAX(f.price) - MIN(f.price) AS price_range,
    stddevPop(f.price) / AVG(f.price) AS price_coefficient_variation,
    -- Additional metrics
    AVG(f.seller_feedback_percentage) AS avg_seller_feedback,
    SUM(CASE WHEN f.free_shipping THEN 1 ELSE 0 END) AS free_shipping_count,
    SUM(CASE WHEN f.free_shipping THEN 0 ELSE 1 END) AS paid_shipping_count
FROM `default`.`fact_listings` f
JOIN `default`.`dim_product` p ON f.product_key = p.product_key
GROUP BY p.product_type, p.weather_bucket
ORDER BY p.product_type, p.weather_bucket
  