
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `default_dbt_test__audit`.`not_null_dim_product_weather_bucket`
    
    ) dbt_internal_test