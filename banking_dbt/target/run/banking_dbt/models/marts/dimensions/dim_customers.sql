
  
    

  create  table "banking"."cleaned"."dim_customers__dbt_tmp"
  
  
    as
  
  (
    

with latest as (
    select
        customer_id,
        first_name,
        last_name,
        email,
        created_at,
        dbt_valid_from      as effective_from,
        dbt_valid_to        as effective_to,
        case 
            when dbt_valid_to is null then 1 
            else 0 
        end                 as is_current
    from "banking"."cleaned"."customers_snapshot"
)
select * from latest
  );
  