SELECT DISTINCT
    city,
    postal_prefix
FROM {{ ref('silver_weather') }}