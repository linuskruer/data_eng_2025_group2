
    
    

select
    location_name as unique_field,
    count(*) as n_records

from default.dim_location
where location_name is not null
group by location_name
having count(*) > 1


