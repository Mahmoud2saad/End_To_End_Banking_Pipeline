{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('silver', 'out_of_cash') }}
)

select
    atm_id,
    event_date,
    city,
    governorate,
    status,
    _silver_processed_at
from source
