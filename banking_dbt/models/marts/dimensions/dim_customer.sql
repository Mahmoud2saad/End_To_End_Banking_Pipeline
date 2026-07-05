{{ config(materialized='table', tags=['dimensions']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['client_id']) }}   AS customer_key,
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
        CASE
            WHEN debt_to_income_ratio > 0.5 THEN 'HIGH_RISK'
            WHEN debt_to_income_ratio > 0.3 THEN 'MEDIUM_RISK'
            ELSE 'LOW_RISK'
        END                                                     AS debt_risk_level,
        CASE
            WHEN num_credit_cards >= 5 THEN 'HEAVY_USER'
            WHEN num_credit_cards >= 3 THEN 'MODERATE_USER'
            ELSE 'LIGHT_USER'
        END                                                     AS credit_usage_tier,
        country,
        currency,
        _silver_loaded_at
    FROM stg
)

SELECT * FROM enriched