{% snapshot cards_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='card_id',
        strategy='timestamp',
        updated_at='_silver_loaded_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    card_id,
    client_id,
    card_brand,
    card_type,
    card_number_masked,
    has_chip,
    num_cards_issued,
    credit_limit_mad,
    acct_open_date,
    year_pin_last_changed,
    card_on_dark_web,
    expires_date,
    is_expired,
    card_age_years,
    dark_web_risk,
    card_category,
    currency,
    _silver_loaded_at
FROM {{ ref('stg_cards') }}

{% endsnapshot %}