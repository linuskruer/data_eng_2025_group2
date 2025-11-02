
  
    
    
        
        insert into default.dim_location ("location_name", "state_code", "zip_prefix", "country_code")
  


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
FROM default.silver_ebay_listings
WHERE item_location_cleaned IS NOT NULL
  AND item_location_cleaned != 'Unknown'

  