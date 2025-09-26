{{ config(materialized='table') }}

with f as (
    select * from {{ ref('fact_bookings') }}
),

agg as (
    select
        f.requested_date as date,
        f.city,
        count(1) as bookings_total,
        sum(f.is_completed) as bookings_completed,
        sum(f.is_canceled) as bookings_canceled,
        avg(f.minutes_to_assign) as avg_minutes_to_assign,
        avg(f.minutes_to_complete) as avg_minutes_to_complete
    from f
    group by 1, 2
),

scored as (
    select
        a.*,
        case when a.bookings_total = 0 then null else round(100.0 * a.bookings_completed / a.bookings_total, 2) end as fill_rate_pct,
        case when a.bookings_total = 0 then null else round(100.0 * a.bookings_canceled / a.bookings_total, 2) end as cancellation_rate_pct
    from agg a
)

select
    s.date,
    s.city,
    s.bookings_total,
    s.bookings_completed,
    s.bookings_canceled,
    s.fill_rate_pct,
    s.avg_minutes_to_assign,
    s.avg_minutes_to_complete,
    w.temp_max,
    w.temp_min,
    w.precipitation,
    w.windspeed_max
from scored s
left join {{ ref('stg_weather') }} w
    on s.city = w.city
    and s.date = w.date
