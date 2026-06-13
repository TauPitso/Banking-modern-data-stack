

with ranked as (
    select
        id::text                as account_id,
        customer_id::text       as customer_id,
        account_type::text      as account_type,
        balance::numeric        as balance,
        currency::text          as currency,
        created_at::timestamp   as created_at,
        current_timestamp       as load_timestamp,
        row_number() over (
            partition by id
            order by created_at desc
        ) as rn
    from "banking"."raw"."accounts"
)
select
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    load_timestamp
from ranked
where rn = 1