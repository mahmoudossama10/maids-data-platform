{% snapshot workers_snapshot %}
{{
config(
  target_schema='STAGING',
  unique_key='worker_id',
  strategy='timestamp',
  updated_at='updated_at'
)
}}
select
  worker_id,
  worker_name,
  worker_type,
  city,
  is_active,
  created_at,
  updated_at
from {{ source('raw','workers') }}
{% endsnapshot %}
