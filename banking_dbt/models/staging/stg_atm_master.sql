{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('silver', 'atm_master') }}
)

select
    atm_id,
    region,
    governorate,
    city,
    daily_limit_mad as daily_limit,
    status,
    _silver_processed_at
from source
