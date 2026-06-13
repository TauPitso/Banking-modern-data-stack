

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    current_timestamp       as load_timestamp
from "banking"."cleaned"."stg_transactions" t
left join "banking"."cleaned"."stg_accounts" a
    on t.account_id = a.account_id


    where t.transaction_time > (select max(transaction_time) from "banking"."cleaned"."fact_transactions")
