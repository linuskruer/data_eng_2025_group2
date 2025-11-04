{{ config(enabled=var('enable_ebay_silver', false)) }}

{% if var('enable_ebay_silver', false) %}
SELECT DISTINCT
    currency_cleaned AS currency_code,
    CASE 
        WHEN currency_cleaned = 'USD' THEN 'US Dollar'
        WHEN currency_cleaned = 'GBP' THEN 'British Pound'
        WHEN currency_cleaned = 'EUR' THEN 'Euro'
        WHEN currency_cleaned = 'CAD' THEN 'Canadian Dollar'
        ELSE currency_cleaned || ' Currency'
    END AS currency_name
FROM {{ ref('silver_ebay_listings') }}
WHERE currency_cleaned IS NOT NULL
{% else %}
SELECT '' AS currency_code, '' AS currency_name WHERE 1=0
{% endif %}

