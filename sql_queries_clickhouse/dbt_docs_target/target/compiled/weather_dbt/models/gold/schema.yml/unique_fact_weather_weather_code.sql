
    
    

select
    weather_code as unique_field,
    count(*) as n_records

from default.fact_weather
where weather_code is not null
group by weather_code
having count(*) > 1


