{{ config(materialized='table') }}
select
    worker_id,
    worker_name,
    worker_type,
    city,
    is_active,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then 1 else 0 end as is_current
from {{ ref('workers_snapshot') }}
