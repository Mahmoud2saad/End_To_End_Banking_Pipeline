-- staging/stg_cards.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'cards') }}
),

renamed AS (
    SELECT
        CAST(card_id AS VARCHAR)                AS card_id,
        CAST(client_id AS VARCHAR)              AS client_id,
        UPPER(TRIM(card_brand))                 AS card_brand,
        UPPER(TRIM(card_type))                  AS card_type,
        TRIM(card_number_masked)                AS card_number_masked,
        COALESCE(CAST(has_chip AS BOOLEAN), FALSE)    AS has_chip,
        COALESCE(CAST(num_cards_issued AS INTEGER), 1) AS num_cards_issued,
        CAST(credit_limit AS DOUBLE)            AS credit_limit_mad,
        TRY_CAST(acct_open_date AS DATE)        AS acct_open_date,
        CAST(year_pin_last_changed AS INTEGER)  AS year_pin_last_changed,
        COALESCE(CAST(card_on_dark_web AS BOOLEAN), FALSE) AS card_on_dark_web,
        TRY_CAST(expires_date AS DATE)          AS expires_date,
        COALESCE(CAST(is_expired AS BOOLEAN), FALSE)   AS is_expired,
        COALESCE(CAST(card_age_years AS INTEGER), 0)   AS card_age_years,
        TRIM(dark_web_risk)                     AS dark_web_risk,
        TRIM(card_category)                     AS card_category,
        COALESCE(currency, 'MAD')               AS currency,
        _silver_loaded_at
    FROM source
    WHERE card_id IS NOT NULL
      AND client_id IS NOT NULL
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY _silver_loaded_at DESC) = 1