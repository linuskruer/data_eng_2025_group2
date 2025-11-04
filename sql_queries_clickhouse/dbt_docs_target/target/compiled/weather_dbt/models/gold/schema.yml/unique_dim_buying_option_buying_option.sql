
    
    

select
    buying_option as unique_field,
    count(*) as n_records

from default.dim_buying_option
where buying_option is not null
group by buying_option
having count(*) > 1


