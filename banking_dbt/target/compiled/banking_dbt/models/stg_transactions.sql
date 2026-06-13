

select
    id::text                    as transaction_id,
    account_id::text            as account_id,
    amount::numeric             as amount,
    txn_type::text              as transaction_type,
    related_account_id::text    as related_account_id,
    status::text                as status,
    created_at::timestamp       as transaction_time,
    current_timestamp           as load_timestamp
from "banking"."raw"."transactions"