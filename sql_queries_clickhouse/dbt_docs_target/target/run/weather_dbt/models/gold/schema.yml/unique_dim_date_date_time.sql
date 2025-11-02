select
      count(*) as failures,
      case when count(*) != 0
        then 'true' else 'false' end as should_warn,
      case when count(*) != 0
        then 'true' else 'false' end as should_error
    from (
      
    
    

select
    date_time as unique_field,
    count(*) as n_records

from default.dim_date
where date_time is not null
group by date_time
having count(*) > 1



      
    ) as dbt_internal_test