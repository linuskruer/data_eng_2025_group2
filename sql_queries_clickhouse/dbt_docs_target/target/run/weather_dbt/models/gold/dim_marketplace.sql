
  
    
    
        
        insert into default.dim_marketplace ("marketplace_id", "marketplace_name")
  


SELECT DISTINCT
    marketplace_id_cleaned AS marketplace_id,
    CASE 
        WHEN marketplace_id_cleaned = 'EBAY_US' THEN 'eBay United States'
        WHEN marketplace_id_cleaned = 'EBAY_GB' THEN 'eBay United Kingdom'
        WHEN marketplace_id_cleaned = 'EBAY_CA' THEN 'eBay Canada'
        ELSE 'eBay ' || marketplace_id_cleaned
    END AS marketplace_name
FROM default.silver_ebay_listings
WHERE marketplace_id_cleaned IS NOT NULL

  