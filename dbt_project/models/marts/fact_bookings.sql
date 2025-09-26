{{ config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='merge',
    cluster_by=['requested_date','city']
) }}

with b as (
    -- No incremental filter; MERGE handles updates vs inserts
    select * from {{ ref('stg_bookings') }}
),
joined as (
    select
        b.booking_id,
        b.customer_id,
        b.worker_id,
        b.city,
        b.channel,
        b.status,
        b.price,
        b.requested_at,
        b.assigned_at,
        b.completed_at,
        b.canceled_at,
        b.updated_at,
        datediff('minute', b.requested_at, b.assigned_at) as minutes_to_assign,
        case when b.completed_at is not null then datediff('minute', b.assigned_at, b.completed_at) end as minutes_to_complete,
        case when b.status = 'completed' then 1 else 0 end as is_completed,
        case when b.status = 'canceled' then 1 else 0 end as is_canceled,
        to_date(b.requested_at) as requested_date
    from b
)
select * from joined
