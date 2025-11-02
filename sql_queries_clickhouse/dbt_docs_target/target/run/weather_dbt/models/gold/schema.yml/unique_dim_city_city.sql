select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
    

select
    city as unique_field,
    count(*) as n_records

from default.dim_city
where city is not null
group by city
having count(*) > 1



      
    ) as dbt_internal_test