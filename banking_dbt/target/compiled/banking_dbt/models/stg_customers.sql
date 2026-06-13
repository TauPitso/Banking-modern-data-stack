

with ranked as (
    select
        id::text                as customer_id,
        first_name::text        as first_name,
        last_name::text         as last_name,
        email::text             as email,
        created_at::timestamp   as created_at,
        current_timestamp       as load_timestamp,
        row_number() over (
            partition by id
            order by created_at desc
        ) as rn
    from "banking"."raw"."customers"
)
select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    load_timestamp
from ranked
where rn = 1