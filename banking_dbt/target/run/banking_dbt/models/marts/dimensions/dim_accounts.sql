
  
    

  create  table "banking"."cleaned"."dim_accounts__dbt_tmp"
  
  
    as
  
  (
    

with source_data as (
    select
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        created_at,
        dbt_valid_from      as effective_from,
        dbt_valid_to        as effective_to,
        case 
            when dbt_valid_to is null then 1 
            else 0 
        end                 as is_current
    from "banking"."cleaned"."accounts_snapshot"
)
select * from source_data
  );
  