{{ config(enabled=var('enable_ebay_silver', false)) }}

{% if var('enable_ebay_silver', false) %}
SELECT DISTINCT
    marketplace_id_cleaned AS marketplace_id,
    CASE 
        WHEN marketplace_id_cleaned = 'EBAY_US' THEN 'eBay United States'
        WHEN marketplace_id_cleaned = 'EBAY_GB' THEN 'eBay United Kingdom'
        WHEN marketplace_id_cleaned = 'EBAY_CA' THEN 'eBay Canada'
        ELSE 'eBay ' || marketplace_id_cleaned
    END AS marketplace_name
FROM {{ ref('silver_ebay_listings') }}
WHERE marketplace_id_cleaned IS NOT NULL
{% else %}
SELECT '' AS marketplace_id, '' AS marketplace_name WHERE 1=0
{% endif %}

