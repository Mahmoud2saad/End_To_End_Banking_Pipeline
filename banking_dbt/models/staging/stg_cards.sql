{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('silver', 'cards_data') }}
)

select
    card_id,
    customer_id,
    card_type,
    card_status,
    credit_limit_mad as credit_limit,
    issue_date,
    expiry_date,
    _silver_processed_at
from source
