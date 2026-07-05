{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='client_id',
        strategy='timestamp',
        updated_at='_silver_loaded_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    client_id,
    current_age,
    retirement_age,
    birth_year,
    birth_month,
    gender,
    address,
    latitude,
    longitude,
    per_capita_income_mad,
    yearly_income_mad,
    total_debt_mad,
    credit_score,
    num_credit_cards,
    age_group,
    income_segment,
    credit_tier,
    debt_to_income_ratio,
    country,
    currency,
    _silver_loaded_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}