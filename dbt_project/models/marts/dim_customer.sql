{{ config(materialized='table') }}

select
    c.customer_id,
    c.full_name,
    c.email,
    c.phone,
    c.city,
    c.created_at,
    c.updated_at
from {{ ref('stg_customers') }} c
