-- Gold Layer: Fact Table - eBay Listings
-- This is the main fact table that connects all dimensions
{{ config(
    materialized='table',
    partition_by='toYYYYMM(collection_timestamp)',
    order_by='(collection_timestamp, item_id)',
    docs={'node_color': 'blue'}
) }}

SELECT 
    -- Primary Key
    item_id,
    
    -- Foreign Keys (surrogate keys from dimensions)
    cityHash64(product_type, weather_category) as product_key,
    cityHash64(postal_code, country) as location_key,
    cityHash64(seller_feedback_score, seller_feedback_percentage) as seller_key,
    
    -- Date Dimension
    collection_timestamp,
    toDate(collection_timestamp) as date_key,
    
    -- Measures
    price,
    shipping_cost,
    seller_feedback_percentage,
    seller_feedback_score,
    title_length,
    
    -- Flags
    free_shipping,
    price_quality_flag,
    feedback_quality_flag,
    
    -- Metadata
    condition,
    buying_options,
    marketplace_id,
    data_source,
    created_at,
    updated_at
    
FROM {{ ref('silver_ebay_data') }}
WHERE collection_timestamp IS NOT NULL
  AND item_id IS NOT NULL
  AND price > 0
  AND seller_feedback_score IS NOT NULL
  AND seller_feedback_percentage IS NOT NULL