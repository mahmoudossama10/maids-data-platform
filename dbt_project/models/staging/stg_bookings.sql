with src as (
    select
        trim(booking_id) as booking_id,
        trim(customer_id) as customer_id,
        trim(worker_id) as worker_id,
        city,
        channel,
        lower(status) as status,
        price::number(10,2) as price,
        cast(requested_at as timestamp_ntz) as requested_at,
        cast(assigned_at as timestamp_ntz) as assigned_at,
        cast(completed_at as timestamp_ntz) as completed_at,
        cast(canceled_at as timestamp_ntz) as canceled_at,
        cast(updated_at as timestamp_ntz) as updated_at
    from {{ source('raw','bookings') }}
),
dedup as (
    select *,
           row_number() over (partition by booking_id order by updated_at desc nulls last) as rn
    from src
)
select booking_id, customer_id, worker_id, city, channel, status, price,
       requested_at, assigned_at, completed_at, canceled_at, updated_at
from dedup
where rn = 1
