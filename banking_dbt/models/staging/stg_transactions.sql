-- staging/stg_transactions.sql
-- Kaggle financial transactions with fraud labels

{{ config(materialized='view', tags=['staging']) }}

WITH source AS (
    SELECT * FROM {{ source('silver', 'kaggle_transactions') }}
),

renamed AS (
    SELECT
        CAST(transaction_id AS VARCHAR)                 AS transaction_id,
        CAST(client_id AS VARCHAR)                      AS client_id,
        CAST(card_id AS VARCHAR)                        AS card_id,
        TRY_CAST(transaction_datetime AS TIMESTAMP)     AS transaction_datetime,
        TRY_CAST(transaction_date AS DATE)              AS transaction_date,
        CAST(transaction_hour AS INTEGER)               AS transaction_hour,
        CAST(amount_mad AS DOUBLE)                      AS amount_mad,
        CAST(amount_abs AS DOUBLE)                      AS amount_abs,
        COALESCE(CAST(is_negative_amount AS BOOLEAN), FALSE) AS is_negative_amount,
        TRIM(use_chip)                                  AS use_chip,
        CAST(merchant_id AS VARCHAR)                    AS merchant_id,
        TRIM(merchant_city)                             AS merchant_city,
        TRIM(merchant_region)                           AS merchant_region,
        CAST(zip AS VARCHAR)                            AS zip,
        CAST(mcc AS VARCHAR)                            AS mcc,
        CAST(errors AS VARCHAR)                         AS errors,
        COALESCE(CAST(is_online AS BOOLEAN), FALSE)         AS is_online,
        COALESCE(CAST(is_chip_transaction AS BOOLEAN), FALSE) AS is_chip_transaction,
        TRIM(amount_bucket)                             AS amount_bucket,
        TRIM(channel)                                   AS channel,
        COALESCE(CAST(is_fraud AS BOOLEAN), FALSE)      AS is_fraud,
        COALESCE(country, 'Morocco')                      AS country,
        COALESCE(currency, 'MAD')                       AS currency,
        _silver_loaded_at
    FROM source
    WHERE transaction_id IS NOT NULL
      AND client_id IS NOT NULL
)

SELECT * FROM renamed
QUALIFY ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY _silver_loaded_at DESC) = 1