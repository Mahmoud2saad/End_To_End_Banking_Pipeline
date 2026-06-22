{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('silver', 'wallet') }}
)

select
    wallet_id,
    customer_id,
    amount_mad as amount,
    transaction_date,
    city,
    channel,
    _silver_processed_at
from source
