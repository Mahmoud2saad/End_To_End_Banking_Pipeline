-- staging/stg_wallet_transactions.sql

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'wallet_transactions') }}
),

renamed AS (
    SELECT
        TRIM(transaction_id)                            AS transaction_id,
        TRIM(mobile_number_masked)                      AS mobile_number_masked,
        UPPER(TRIM(terminal_id))                        AS atm_id,
        TRY_CAST(transaction_datetime AS TIMESTAMP)     AS transaction_datetime,
        TRY_CAST(transaction_date AS DATE)              AS transaction_date,
        CAST(transaction_hour AS INTEGER)               AS transaction_hour,
        TRIM(transaction_type)                          AS transaction_type,
        CAST(amount_mad AS DOUBLE)                      AS amount_mad,
        TRIM(transaction_status)                        AS transaction_status,
        COALESCE(CAST(is_reversal AS BOOLEAN), FALSE)   AS is_reversal,
        COALESCE(CAST(is_cash_out AS BOOLEAN), FALSE)   AS is_cash_out,
        COALESCE(CAST(is_successful AS BOOLEAN), FALSE) AS is_successful,
        COALESCE(channel, 'MOBILE_WALLET')              AS channel,
        COALESCE(currency, 'MAD')                       AS currency,
        _silver_loaded_at
    FROM source
    WHERE transaction_id IS NOT NULL
      AND amount_mad >= 0
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _silver_loaded_at DESC) = 1