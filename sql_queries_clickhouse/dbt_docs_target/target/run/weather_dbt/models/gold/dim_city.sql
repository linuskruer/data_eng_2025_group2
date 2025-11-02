
  
    
    
        
        insert into default.dim_city__dbt_backup ("city", "postal_prefix")
  SELECT DISTINCT
    city,
    postal_prefix
FROM default.silver_weather
  