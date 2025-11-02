
    
    

select
    city as unique_field,
    count(*) as n_records

from default.dim_city
where city is not null
group by city
having count(*) > 1


