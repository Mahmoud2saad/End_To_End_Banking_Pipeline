-- staging/stg_customers.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'customers') }}
),

renamed AS (
    SELECT
        CAST(client_id AS VARCHAR)          AS client_id,
        CAST(current_age AS INTEGER)        AS current_age,
        CAST(retirement_age AS INTEGER)     AS retirement_age,
        CAST(birth_year AS INTEGER)         AS birth_year,
        CAST(birth_month AS INTEGER)        AS birth_month,
        UPPER(TRIM(gender))                 AS gender,
        TRIM(address)                       AS address,
        CAST(latitude AS DOUBLE)            AS latitude,
        CAST(longitude AS DOUBLE)           AS longitude,
        CAST(per_capita_income AS DOUBLE)   AS per_capita_income_mad,
        CAST(yearly_income AS DOUBLE)       AS yearly_income_mad,
        CAST(total_debt AS DOUBLE)          AS total_debt_mad,
        CAST(credit_score AS INTEGER)       AS credit_score,
        CAST(num_credit_cards AS INTEGER)   AS num_credit_cards,
        TRIM(age_group)                     AS age_group,
        TRIM(income_segment)                AS income_segment,
        TRIM(credit_tier)                   AS credit_tier,
        CAST(debt_to_income_ratio AS DOUBLE) AS debt_to_income_ratio,
        COALESCE(country, 'Morocco')          AS country,
        COALESCE(currency, 'MAD')           AS currency,
        _silver_loaded_at
    FROM source
    WHERE client_id IS NOT NULL
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY _silver_loaded_at DESC) = 1