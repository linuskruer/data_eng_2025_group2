{{ config(enabled=var('enable_ebay_silver', false)) }}

{% if var('enable_ebay_silver', false) %}
SELECT DISTINCT
    item_location_cleaned AS location_name,
    -- Extract state code if available (simple heuristic)
    CASE 
        WHEN match(item_location_cleaned, '[A-Z]{2}') THEN 
            extract(item_location_cleaned, '[A-Z]{2}')
        ELSE NULL
    END AS state_code,
    -- Extract zip code prefix if available
    CASE 
        WHEN match(item_location_cleaned, '\\d{5}') THEN 
            substring(toString(extract(item_location_cleaned, '\\d{5}')), 1, 3)
        WHEN match(item_location_cleaned, '\\d{3}') THEN 
            toString(extract(item_location_cleaned, '\\d{3}'))
        ELSE NULL
    END AS zip_prefix,
    'US' AS country_code
FROM {{ ref('silver_ebay_listings') }}
WHERE item_location_cleaned IS NOT NULL
  AND item_location_cleaned != 'Unknown'
{% else %}
SELECT '' AS location_name, NULL AS state_code, NULL AS zip_prefix, '' AS country_code WHERE 1=0
{% endif %}

