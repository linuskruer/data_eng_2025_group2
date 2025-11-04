
    
    

select
    marketplace_id as unique_field,
    count(*) as n_records

from default.dim_marketplace
where marketplace_id is not null
group by marketplace_id
having count(*) > 1


