
  
    
    
        
        insert into default.dim_condition ("condition_name")
  


SELECT DISTINCT
    condition_cleaned AS condition_name
FROM default.silver_ebay_listings
WHERE condition_cleaned IS NOT NULL
  AND condition_cleaned != 'Unknown'

  