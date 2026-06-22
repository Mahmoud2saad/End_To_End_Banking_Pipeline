{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('silver', 'users_data') }}
)

select
    customer_id,
    full_name,
    gender,
    city,
    region,
    age,
    income_mad as income,
    phone,
    email,
    _silver_processed_at
from source
