
    
    

select
    weather_key as unique_field,
    count(*) as n_records

from `default`.`dim_weather`
where weather_key is not null
group by weather_key
having count(*) > 1


