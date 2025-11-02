{{ config(
    materialized='incremental',
    unique_key='item_id_collection_ts',
    enabled=var('enable_ebay_silver', false)
) }}

{% if var('enable_ebay_silver', false) %}
WITH cleaned AS (
    SELECT
        -- Core identifiers
        item_id,
        collection_timestamp,
        concat(item_id, '_', toString(collection_timestamp)) AS item_id_collection_ts,
        
        -- Product information
        product_type,
        CASE 
            WHEN product_type = '' OR product_type IS NULL THEN 'Unknown'
            ELSE trimBoth(product_type)
        END AS product_type_cleaned,
        
        -- Pricing
        price,
        CASE 
            WHEN price IS NULL OR price <= 0 THEN NULL
            ELSE price
        END AS price_cleaned,
        currency,
        CASE 
            WHEN currency = '' OR currency IS NULL THEN 'USD'
            ELSE upperUTF8(trimBoth(currency))
        END AS currency_cleaned,
        
        -- Seller information
        seller_feedback_score,
        CASE 
            WHEN seller_feedback_score IS NULL THEN 0
            WHEN seller_feedback_score < 0 THEN 0
            ELSE seller_feedback_score
        END AS seller_feedback_score_cleaned,
        seller_feedback_percentage,
        CASE 
            WHEN seller_feedback_percentage IS NULL THEN 0.0
            WHEN seller_feedback_percentage < 0 THEN 0.0
            WHEN seller_feedback_percentage > 100 THEN 100.0
            ELSE seller_feedback_percentage
        END AS seller_feedback_percentage_cleaned,
        seller_location,
        CASE 
            WHEN seller_location = '' OR seller_location IS NULL THEN 'Unknown'
            ELSE trimBoth(seller_location)
        END AS seller_location_cleaned,
        
        -- Item location
        item_location,
        CASE 
            WHEN item_location = '' OR item_location IS NULL THEN 'Unknown'
            ELSE trimBoth(item_location)
        END AS item_location_cleaned,
        
        -- Shipping
        shipping_cost,
        CASE 
            WHEN shipping_cost IS NULL THEN 0.0
            WHEN shipping_cost < 0 THEN 0.0
            ELSE shipping_cost
        END AS shipping_cost_cleaned,
        free_shipping,
        CASE 
            WHEN free_shipping = 1 THEN true
            WHEN free_shipping = 0 THEN false
            WHEN shipping_cost = 0 OR shipping_cost IS NULL THEN true
            ELSE false
        END AS free_shipping_cleaned,
        
        -- Item details
        condition,
        CASE 
            WHEN condition = '' OR condition IS NULL THEN 'Unknown'
            ELSE trimBoth(condition)
        END AS condition_cleaned,
        buying_options,
        CASE 
            WHEN buying_options = '' OR buying_options IS NULL THEN 'Unknown'
            ELSE trimBoth(buying_options)
        END AS buying_options_cleaned,
        title_length,
        CASE 
            WHEN title_length IS NULL THEN 0
            WHEN title_length < 0 THEN 0
            ELSE title_length
        END AS title_length_cleaned,
        
        -- Metadata
        weather_category,
        CASE 
            WHEN weather_category = '' OR weather_category IS NULL THEN 'Normal'
            ELSE trimBoth(weather_category)
        END AS weather_category_cleaned,
        marketplace_id,
        CASE 
            WHEN marketplace_id = '' OR marketplace_id IS NULL THEN 'EBAY_US'
            ELSE trimBoth(marketplace_id)
        END AS marketplace_id_cleaned,
        timezone,
        
        -- Extract date for partitioning
        toDate(collection_timestamp) AS collection_date
    FROM {{ source('bronze', 'ebay_raw_data') }}
    WHERE item_id IS NOT NULL
      AND item_id != ''
      AND collection_timestamp IS NOT NULL
      AND (price IS NULL OR price > 0)
)

SELECT * FROM cleaned
{% else %}
-- eBay data not available yet, return empty result
SELECT 
    '' AS item_id,
    now() AS collection_timestamp,
    '' AS item_id_collection_ts,
    '' AS product_type_cleaned,
    NULL AS price_cleaned,
    '' AS currency_cleaned,
    0 AS seller_feedback_score_cleaned,
    0.0 AS seller_feedback_percentage_cleaned,
    '' AS seller_location_cleaned,
    '' AS item_location_cleaned,
    0.0 AS shipping_cost_cleaned,
    false AS free_shipping_cleaned,
    '' AS condition_cleaned,
    '' AS buying_options_cleaned,
    0 AS title_length_cleaned,
    '' AS weather_category_cleaned,
    '' AS marketplace_id_cleaned,
    '' AS timezone,
    today() AS collection_date
WHERE 1=0  -- Return no rows
{% endif %}

