with src as (
    select
        trim(worker_id) as worker_id,
        worker_name,
        worker_type,
        city,
        is_active,
        cast(created_at as timestamp_ntz) as created_at,
        cast(updated_at as timestamp_ntz) as updated_at
    from {{ source('raw','workers') }}
),
dedup as (
    select *,
           row_number() over (partition by worker_id order by updated_at desc nulls last) as rn
    from src
)
select worker_id, worker_name, worker_type, city, is_active, created_at, updated_at
from dedup
where rn = 1
