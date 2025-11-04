
    
    

select
    product_type as unique_field,
    count(*) as n_records

from default.dim_product
where product_type is not null
group by product_type
having count(*) > 1


