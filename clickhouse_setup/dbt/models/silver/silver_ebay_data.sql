-- Silver Layer Models for dbt
-- These models clean and standardize the bronze data

-- Silver: Clean eBay Data
{{ config(
    materialized='table',
    partition_by='toYYYYMM(collection_timestamp)',
    order_by='(collection_timestamp, item_id)'
) }}

SELECT 
    collection_timestamp,
    timezone,
    weather_category,
    product_type,
    price,
    currency,
    seller_feedback_percentage,
    seller_feedback_score,
    -- Parse JSON location data
    JSONExtractString(item_location, 'postalCode') as postal_code,
    JSONExtractString(item_location, 'country') as country,
    seller_location,
    shipping_cost,
    free_shipping,
    condition,
    buying_options,
    title_length,
    item_id,
    marketplace_id,
    execution_date,
    data_source,
    created_at,
    updated_at,
    -- Add data quality flags
    CASE 
        WHEN price <= 0 THEN 1 
        ELSE 0 
    END as price_quality_flag,
    CASE 
        WHEN seller_feedback_percentage < 0 OR seller_feedback_percentage > 100 THEN 1 
        ELSE 0 
    END as feedback_quality_flag
FROM {{ source('bronze', 'ebay_raw_data') }}
WHERE collection_timestamp IS NOT NULL
  AND item_id IS NOT NULL
  AND price > 0
