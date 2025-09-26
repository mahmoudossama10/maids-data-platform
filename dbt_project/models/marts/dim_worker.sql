{{ config(materialized='table') }}

select
    w.worker_id,
    w.worker_name,
    w.worker_type,
    w.city,
    w.is_active,
    w.created_at,
    w.updated_at
from {{ ref('stg_workers') }} w
