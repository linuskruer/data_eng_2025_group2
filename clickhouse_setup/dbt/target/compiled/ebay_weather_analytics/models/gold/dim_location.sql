-- Gold Layer: Dimension Table - Locations
-- Location dimension with East Coast mapping


WITH location_data AS (
    SELECT DISTINCT
        postal_code,
        country,
        CASE 
            WHEN postal_code LIKE '100%' OR postal_code LIKE '101%' OR postal_code LIKE '102%' OR postal_code LIKE '103%' OR postal_code LIKE '104%' THEN 'New York'
            WHEN postal_code LIKE '111%' OR postal_code LIKE '112%' OR postal_code LIKE '113%' OR postal_code LIKE '114%' THEN 'New York'
            WHEN postal_code LIKE '331%' OR postal_code LIKE '332%' THEN 'Miami'
            WHEN postal_code LIKE '021%' OR postal_code LIKE '022%' THEN 'Boston'
            WHEN postal_code LIKE '191%' OR postal_code LIKE '192%' THEN 'Philadelphia'
            WHEN postal_code LIKE '200%' OR postal_code LIKE '202%' THEN 'Washington DC'
            ELSE 'Other East Coast'
        END AS city,
        CASE 
            WHEN postal_code LIKE '100%' OR postal_code LIKE '101%' OR postal_code LIKE '102%' OR postal_code LIKE '103%' OR postal_code LIKE '104%' OR
                 postal_code LIKE '111%' OR postal_code LIKE '112%' OR postal_code LIKE '113%' OR postal_code LIKE '114%' THEN 'NY'
            WHEN postal_code LIKE '331%' OR postal_code LIKE '332%' THEN 'FL'
            WHEN postal_code LIKE '021%' OR postal_code LIKE '022%' THEN 'MA'
            WHEN postal_code LIKE '191%' OR postal_code LIKE '192%' THEN 'PA'
            WHEN postal_code LIKE '200%' OR postal_code LIKE '202%' THEN 'DC'
            ELSE 'EC'
        END AS state_code
    FROM `default`.`silver_ebay_data`
    WHERE postal_code IS NOT NULL
      AND postal_code != ''
)

SELECT 
    -- Surrogate Key
    cityHash64(postal_code, country) as location_key,
    
    -- Natural Keys
    postal_code,
    country,
    
    -- Attributes
    city,
    state_code,
    'US' as country_code,
    'East Coast' as region,
    
    -- Geographic attributes
    CASE 
        WHEN state_code IN ('NY', 'NJ', 'CT', 'MA', 'RI', 'VT', 'NH', 'ME') THEN 'Northeast'
        WHEN state_code IN ('PA', 'DE', 'MD', 'DC', 'VA', 'WV') THEN 'Mid-Atlantic'
        WHEN state_code IN ('NC', 'SC', 'GA', 'FL') THEN 'Southeast'
        ELSE 'Other'
    END AS region_group,
    
    -- Metadata
    'active' as status,
    now() as created_at,
    now() as updated_at
    
FROM location_data