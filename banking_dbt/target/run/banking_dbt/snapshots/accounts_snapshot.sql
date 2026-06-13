
      update "banking"."cleaned"."accounts_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "accounts_snapshot__dbt_tmp103006286407" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "banking"."cleaned"."accounts_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "banking"."cleaned"."accounts_snapshot".dbt_valid_to is null;
      


    insert into "banking"."cleaned"."accounts_snapshot" ("account_id", "customer_id", "account_type", "balance", "currency", "created_at", "load_timestamp", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."account_id",DBT_INTERNAL_SOURCE."customer_id",DBT_INTERNAL_SOURCE."account_type",DBT_INTERNAL_SOURCE."balance",DBT_INTERNAL_SOURCE."currency",DBT_INTERNAL_SOURCE."created_at",DBT_INTERNAL_SOURCE."load_timestamp",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "accounts_snapshot__dbt_tmp103006286407" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  