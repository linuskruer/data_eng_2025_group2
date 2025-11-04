
  
    
    
        
        insert into default.dim_date__dbt_backup ("date_time", "date", "day_of_week", "month", "year")
  SELECT DISTINCT
    time AS date_time,
    toDate(time) AS date,
    toDayOfWeek(time) AS day_of_week,
    toMonth(time) AS month,
    toYear(time) AS year
FROM default.silver_weather
  