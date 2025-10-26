
    
    

select
    date as unique_field,
    count(*) as n_records

from `default`.`dim_weather`
where date is not null
group by date
having count(*) > 1


