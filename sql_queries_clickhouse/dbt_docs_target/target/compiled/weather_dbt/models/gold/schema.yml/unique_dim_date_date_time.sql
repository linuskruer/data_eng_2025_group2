
    
    

select
    date_time as unique_field,
    count(*) as n_records

from default.dim_date
where date_time is not null
group by date_time
having count(*) > 1


