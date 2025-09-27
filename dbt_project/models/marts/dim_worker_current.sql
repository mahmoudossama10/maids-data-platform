{{ config(materialized='table') }}
select
  worker_id,
  worker_name,
  worker_type,
  city,
  is_active,
  valid_from
from {{ ref('dim_worker_history') }}
where is_current = 1
