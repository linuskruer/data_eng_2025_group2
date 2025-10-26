
  
    
    
    
        
         


        insert into `default`.`dim_product`
        ("product_key", "product_type", "weather_category", "weather_bucket", "product_category", "weather_sensitivity", "status", "created_at", "updated_at")-- Gold Layer: Dimension Table - Products
-- Product dimension with weather category mapping


WITH product_data AS (
    SELECT DISTINCT
        product_type,
        weather_category,
        CASE 
            WHEN weather_category = 'rain_products' THEN 'Precipitation-Heavy'
            WHEN weather_category = 'heat_products' THEN 'Extreme Heat'
            WHEN weather_category = 'cold_products' THEN 'Extreme Cold'
            ELSE 'Normal'
        END AS weather_bucket
    FROM `default`.`silver_ebay_data`
    WHERE product_type IS NOT NULL
)

SELECT 
    -- Surrogate Key
    cityHash64(product_type, weather_category) as product_key,
    
    -- Natural Keys
    product_type,
    weather_category,
    weather_bucket,
    
    -- Attributes
    CASE 
        WHEN product_type IN ('umbrella', 'rain jacket') THEN 'Rain Protection'
        WHEN product_type IN ('air conditioner', 'sunscreen') THEN 'Heat Protection'
        WHEN product_type IN ('winter coat', 'thermal gloves') THEN 'Cold Protection'
        WHEN product_type IN ('beach towel', 'snow shovel', 'outdoor furniture') THEN 'Seasonal'
        ELSE 'Other'
    END AS product_category,
    
    CASE 
        WHEN product_type IN ('umbrella', 'rain jacket', 'air conditioner', 'sunscreen', 'winter coat', 'thermal gloves') THEN 'Weather-Sensitive'
        ELSE 'General'
    END AS weather_sensitivity,
    
    -- Metadata
    'active' as status,
    now() as created_at,
    now() as updated_at
    
FROM product_data
  