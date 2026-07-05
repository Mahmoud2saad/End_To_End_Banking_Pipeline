{{ config(materialized='table', tags=['dimensions']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_cards') }}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['card_id']) }}     AS card_key,
        card_id,
        client_id,
        {{ dbt_utils.generate_surrogate_key(['client_id']) }}   AS customer_key,
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
        CASE
            WHEN card_on_dark_web THEN 'CRITICAL'
            WHEN is_expired       THEN 'HIGH'
            WHEN card_age_years > 5 THEN 'MEDIUM'
            ELSE 'LOW'
        END                                                     AS card_risk_level,
        currency,
        _silver_loaded_at
    FROM stg
)

SELECT * FROM enriched