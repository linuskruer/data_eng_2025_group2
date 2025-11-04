{{ config(enabled=var('enable_ebay_silver', false)) }}

{% if var('enable_ebay_silver', false) %}
SELECT DISTINCT
    condition_cleaned AS condition_name
FROM {{ ref('silver_ebay_listings') }}
WHERE condition_cleaned IS NOT NULL
  AND condition_cleaned != 'Unknown'
{% else %}
SELECT '' AS condition_name WHERE 1=0
{% endif %}

