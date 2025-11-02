{{ config(
    enabled=var('enable_ebay_silver', false)
) }}

{% if var('enable_ebay_silver', false) %}
SELECT
    -- Degenerate dimension
    item_id,
    
    -- Foreign keys (will be joined in analytical queries)
    toDate(collection_timestamp) AS date_key,
    product_type_cleaned AS product_type,  -- For joining to dim_product
    item_location_cleaned AS location_name,  -- For joining to dim_location
    seller_feedback_score_cleaned AS seller_feedback_score,  -- For joining to dim_seller
    marketplace_id_cleaned AS marketplace_id,  -- For joining to dim_marketplace
    currency_cleaned AS currency_code,  -- For joining to dim_currency
    condition_cleaned AS condition_name,  -- For joining to dim_condition
    buying_options_cleaned AS buying_option,  -- For joining to dim_buying_option
    
    -- Measures
    price_cleaned AS price,
    shipping_cost_cleaned AS shipping_cost,
    free_shipping_cleaned AS free_shipping,
    title_length_cleaned AS title_length,
    
    -- Additional dimensions for filtering
    collection_timestamp,
    weather_category_cleaned AS weather_category,
    seller_location_cleaned AS seller_location,
    seller_feedback_percentage_cleaned AS seller_feedback_percentage,
    timezone
    
FROM {{ ref('silver_ebay_listings') }}
WHERE price_cleaned IS NOT NULL
{% else %}
SELECT 
    '' AS item_id,
    today() AS date_key,
    '' AS product_type,
    '' AS location_name,
    0 AS seller_feedback_score,
    '' AS marketplace_id,
    '' AS currency_code,
    '' AS condition_name,
    '' AS buying_option,
    NULL AS price,
    NULL AS shipping_cost,
    false AS free_shipping,
    0 AS title_length,
    now() AS collection_timestamp,
    '' AS weather_category,
    '' AS seller_location,
    0.0 AS seller_feedback_percentage,
    '' AS timezone
WHERE 1=0
{% endif %}

