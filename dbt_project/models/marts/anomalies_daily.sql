{{ config(materialized='table') }}

with base as (
    select
        date,
        city,
        bookings_total,
        avg(bookings_total) over (partition by city order by date rows between 30 preceding and 1 preceding) as ma30,
        stddev(bookings_total) over (partition by city order by date rows between 30 preceding and 1 preceding) as sd30
    from {{ ref('metrics_daily') }}
)
select
    date,
    city,
    bookings_total,
    ma30,
    sd30,
    case when sd30 is null or sd30 = 0 then 0 else (bookings_total - ma30) / sd30 end as zscore,
    case when sd30 is not null and sd30 <> 0 and abs((bookings_total - ma30) / sd30) >= 3 then true else false end as is_anomaly
from base
