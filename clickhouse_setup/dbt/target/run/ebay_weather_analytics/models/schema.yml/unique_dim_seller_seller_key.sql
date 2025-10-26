
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `default_dbt_test__audit`.`unique_dim_seller_seller_key`
    
    ) dbt_internal_test