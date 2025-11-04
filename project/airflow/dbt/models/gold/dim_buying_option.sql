{{ config(enabled=var('enable_ebay_silver', false)) }}

{% if var('enable_ebay_silver', false) %}
SELECT DISTINCT
    buying_options_cleaned AS buying_option
FROM {{ ref('silver_ebay_listings') }}
WHERE buying_options_cleaned IS NOT NULL
  AND buying_options_cleaned != 'Unknown'
{% else %}
SELECT '' AS buying_option WHERE 1=0
{% endif %}

