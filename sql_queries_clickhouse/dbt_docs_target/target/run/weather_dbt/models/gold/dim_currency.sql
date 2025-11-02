
  
    
    
        
        insert into default.dim_currency ("currency_code", "currency_name")
  


SELECT DISTINCT
    currency_cleaned AS currency_code,
    CASE 
        WHEN currency_cleaned = 'USD' THEN 'US Dollar'
        WHEN currency_cleaned = 'GBP' THEN 'British Pound'
        WHEN currency_cleaned = 'EUR' THEN 'Euro'
        WHEN currency_cleaned = 'CAD' THEN 'Canadian Dollar'
        ELSE currency_cleaned || ' Currency'
    END AS currency_name
FROM default.silver_ebay_listings
WHERE currency_cleaned IS NOT NULL

  