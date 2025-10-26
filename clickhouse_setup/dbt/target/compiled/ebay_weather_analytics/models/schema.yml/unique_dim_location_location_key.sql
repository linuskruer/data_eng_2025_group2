
    
    

select
    location_key as unique_field,
    count(*) as n_records

from `default`.`dim_location`
where location_key is not null
group by location_key
having count(*) > 1


