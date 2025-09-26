with src as (
    select
        trim(customer_id) as customer_id,
        full_name,
        lower(email) as email,
        phone,
        city,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at
    from {{ source('raw','customers') }}
),
dedup as (
    select *,
           row_number() over (partition by customer_id order by updated_at desc nulls last) as rn
    from src
)
select customer_id, full_name, email, phone, city, created_at, updated_at
from dedup
where rn = 1
