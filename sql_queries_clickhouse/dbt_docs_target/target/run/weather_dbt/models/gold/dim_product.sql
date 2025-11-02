
  
    
    
        
        insert into default.dim_product ("product_type")
  


SELECT DISTINCT
    product_type_cleaned AS product_type
FROM default.silver_ebay_listings
WHERE product_type_cleaned IS NOT NULL
  AND product_type_cleaned != 'Unknown'

  