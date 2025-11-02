select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
    

select
    weather_code as unique_field,
    count(*) as n_records

from default.fact_weather
where weather_code is not null
group by weather_code
having count(*) > 1



      
    ) as dbt_internal_test