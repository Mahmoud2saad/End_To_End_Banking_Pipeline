-- staging/stg_card_transactions.sql
-- ATM card transactions (from silver card_transactions Delta table)
-- NOTE: stg_atm_transactions reads the same physical source but
--       filters/renames for ATM-specific downstream models.
--       This model exposes all card channel transactions for fact_card_transactions.

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'card_transactions') }}
),

renamed AS (
    SELECT
        CAST(refnum           AS VARCHAR)   AS refnum,
        CAST(terminal_id      AS VARCHAR)   AS terminal_id,
        CAST(client_id        AS VARCHAR)   AS client_id,
        TRY_CAST(transaction_date  AS DATE)     AS transaction_date,
        CAST(transaction_hour AS INTEGER)   AS transaction_hour,
        TRIM(transaction_type)              AS transaction_type,
        CAST(msg_type         AS INTEGER)   AS msg_type,
        CAST(amount_mad       AS DOUBLE)    AS amount_mad,
        CAST(resp_code        AS INTEGER)   AS resp_code,
        COALESCE(CAST(is_successful   AS BOOLEAN), FALSE) AS is_successful,
        COALESCE(CAST(is_reversal     AS BOOLEAN), FALSE) AS is_reversal,
        COALESCE(CAST(is_out_of_cash  AS BOOLEAN), FALSE) AS is_out_of_cash,
        COALESCE(CAST(is_deposit      AS BOOLEAN), FALSE) AS is_deposit,
        COALESCE(TRIM(channel), 'CARD_ATM')               AS channel,
        COALESCE(currency, 'MAD')           AS currency,
        _silver_loaded_at
    FROM source
    WHERE refnum IS NOT NULL
      AND CAST(amount_mad AS DOUBLE) >= 0
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY refnum ORDER BY _silver_loaded_at DESC) = 1
