
  
    
    
        
        insert into default.fact_weather__dbt_backup ("time", "city", "postal_prefix", "weather_code", "temperature_2m", "relative_humidity_2m", "cloudcover", "rain", "sunshine_duration", "windspeed_10m")
  SELECT
    time,
    city,
    postal_prefix,
    weather_code,
    temperature_2m,
    relative_humidity_2m,
    cloudcover,
    rain,
    sunshine_duration,
    windspeed_10m
FROM default.silver_weather
  