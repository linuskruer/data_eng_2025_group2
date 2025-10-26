
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
        select *
        from `default_dbt_test__audit`.`unique_fact_listings_item_id`
    
    ) dbt_internal_test