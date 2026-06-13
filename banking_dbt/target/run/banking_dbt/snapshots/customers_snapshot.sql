
      update "banking"."cleaned"."customers_snapshot"
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "customers_snapshot__dbt_tmp103007460916" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = "banking"."cleaned"."customers_snapshot".dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and "banking"."cleaned"."customers_snapshot".dbt_valid_to is null;
      


    insert into "banking"."cleaned"."customers_snapshot" ("customer_id", "first_name", "last_name", "email", "created_at", "load_timestamp", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."customer_id",DBT_INTERNAL_SOURCE."first_name",DBT_INTERNAL_SOURCE."last_name",DBT_INTERNAL_SOURCE."email",DBT_INTERNAL_SOURCE."created_at",DBT_INTERNAL_SOURCE."load_timestamp",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "customers_snapshot__dbt_tmp103007460916" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;

  