-- staging/stg_atm_transactions.sql

{{ config(materialized='view', tags=['staging']) }}

WITH raw AS (
    SELECT * FROM {{ source('silver', 'card_transactions') }}
)

SELECT
    refnum                              AS refnum,
    terminal_id                              AS atm_id,
    CAST(amount_mad AS DOUBLE)          AS amount_mad,
    CAST(transaction_date AS DATE)      AS transaction_date,
    CAST(transaction_hour AS INTEGER)   AS transaction_hour,
    transaction_type                    AS transaction_type,
    CAST(msg_type AS INTEGER)           AS msg_type,
    CAST(resp_code AS INTEGER)          AS resp_code,
    CAST(is_successful AS BOOLEAN)      AS is_successful,
    CAST(is_reversal AS BOOLEAN)        AS is_reversal,
    CAST(is_out_of_cash AS BOOLEAN)     AS is_out_of_cash,
    CAST(is_deposit AS BOOLEAN)         AS is_deposit,
    CAST(client_id AS VARCHAR)          AS client_id,
    channel                             AS channel,
    currency                            AS currency,
    _silver_loaded_at
FROM raw
WHERE refnum IS NOT NULL
  AND CAST(amount_mad AS DOUBLE) >= 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY refnum ORDER BY _silver_loaded_at DESC) = 1
