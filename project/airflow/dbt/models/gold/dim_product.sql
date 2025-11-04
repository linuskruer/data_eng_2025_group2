{{ config(enabled=var('enable_ebay_silver', false)) }}

{% if var('enable_ebay_silver', false) %}
SELECT DISTINCT
    product_type_cleaned AS product_type
FROM {{ ref('silver_ebay_listings') }}
WHERE product_type_cleaned IS NOT NULL
  AND product_type_cleaned != 'Unknown'
{% else %}
SELECT '' AS product_type WHERE 1=0
{% endif %}

