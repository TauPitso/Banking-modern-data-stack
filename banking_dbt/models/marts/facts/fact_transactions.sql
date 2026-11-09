{{ config(materialized='incremental', unique_key='transaction_id') }}

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.related_account_id,
    t.status,
    t.transaction_type,
    t.transaction_time,
    current_timestamp       as load_timestamp,
    CAST(current_timestamp AS DATE) AS snapshot_date
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_accounts') }} a
    on t.account_id = a.account_id

{% if is_incremental() %}
    where t.transaction_time > (select max(transaction_time) from {{ this }})
{% endif %}
