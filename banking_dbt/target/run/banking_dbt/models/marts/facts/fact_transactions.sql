
      
        
        
        delete from "banking"."cleaned"."fact_transactions" as DBT_INTERNAL_DEST
        where (transaction_id) in (
            select distinct transaction_id
            from "fact_transactions__dbt_tmp103044876250" as DBT_INTERNAL_SOURCE
        );

    

    insert into "banking"."cleaned"."fact_transactions" ("transaction_id", "account_id", "customer_id", "amount", "related_account_id", "status", "transaction_type", "transaction_time", "load_timestamp")
    (
        select "transaction_id", "account_id", "customer_id", "amount", "related_account_id", "status", "transaction_type", "transaction_time", "load_timestamp"
        from "fact_transactions__dbt_tmp103044876250"
    )
  