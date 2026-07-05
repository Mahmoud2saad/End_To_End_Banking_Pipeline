-- staging/stg_out_of_cash.sql

{{ config(materialized='view', tags=['staging']) }}

WITH raw AS (
    SELECT * FROM {{ source('silver', 'out_of_cash') }}
),

renamed AS (
    SELECT
        TRIM(refnum)                                    AS refnum,
        TRIM(pan)                                       AS pan_masked,
        UPPER(TRIM(terminal_id))                        AS atm_id,
        TRY_CAST(transaction_date AS DATE)              AS transaction_date,
        CAST(transaction_hour AS INTEGER)               AS transaction_hour,
        CAST(attempted_amount_mad AS DOUBLE)            AS attempted_amount_mad,
        CAST(resp_code AS INTEGER)                      AS resp_code,
        COALESCE(TRIM(failure_reason), 'GUICHET VIDE')  AS failure_reason,
        COALESCE(CAST(is_confirmed_ooc AS BOOLEAN), FALSE) AS is_confirmed_ooc,
        COALESCE(currency, 'MAD')                       AS currency,
        _silver_loaded_at
    FROM raw
    WHERE terminal_id IS NOT NULL
      AND refnum IS NOT NULL
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY refnum ORDER BY _silver_loaded_at DESC) = 1