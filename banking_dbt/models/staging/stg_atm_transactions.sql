{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('silver', 'transactions_data') }}
    where upper(channel) = 'ATM'
)

select
    transaction_id,
    customer_id,
    card_id,
    atm_id,
    amount_mad as amount,
    currency,
    transaction_date,
    transaction_time,
    city,
    channel,
    is_fraudulent,
    _silver_processed_at
from source
