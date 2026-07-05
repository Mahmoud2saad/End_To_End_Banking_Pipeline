-- Meds is going to create the fact table of the card transactions  but it's kinda hard this time because it has a lot of joins + NANs
-- + diffrent dims refrensed here and there is a lot of cleaning to do in the source table before we can join with the dims and create the fact table.
{{ config(
    materialized='incremental',
    unique_key='card_transaction_key',
    incremental_strategy='merge',
    tags=['facts']
) }}

WITH stg AS (
    SELECT
        *,
        TRY_CAST(NULLIF(TRIM(CAST(errors AS VARCHAR)), 'NaN') AS INTEGER) AS error_code_clean
    FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

dim_cust AS (
    SELECT customer_key, client_id
    FROM {{ ref('dim_customer') }}
),

dim_crd AS (
    SELECT card_key, card_id
    FROM {{ ref('dim_card') }}
),

dim_merch AS (
    SELECT merchant_key, merchant_id
    FROM {{ ref('dim_merchant') }}
),

dim_mcc AS (
    SELECT merchant_category_key, mcc_code
    FROM {{ ref('dim_merchant_category') }}
),

dim_ch AS (
    SELECT channel_key, channel_code
    FROM {{ ref('dim_channel') }}
),

dim_err AS (
    SELECT error_type_key, CAST(error_code AS INTEGER) AS error_code
    FROM {{ ref('dim_error_type') }}
),

dim_dt AS (
    SELECT date_key, full_date
    FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stg.transaction_id']) }} AS card_transaction_key,
        dt.date_key,
        dc.customer_key,
        dcard.card_key,
        dm.merchant_key,
        dmcc.merchant_category_key,
        dch.channel_key,
        derr.error_type_key,
        stg.transaction_id,
        stg.amount_mad,
        stg.amount_abs,
        stg.is_fraud,
        stg.is_online,
        stg.is_chip_transaction,
        stg.is_negative_amount,
        stg.amount_bucket,
        stg.transaction_date,
        stg.transaction_hour,
        stg.mcc AS mcc_code,
        stg.errors,
        stg.country,
        stg.currency,
        stg._silver_loaded_at,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM stg
    LEFT JOIN dim_cust dc
        ON stg.client_id = dc.client_id
    LEFT JOIN dim_crd dcard
        ON stg.card_id = dcard.card_id
    LEFT JOIN dim_merch dm
        ON stg.merchant_id = dm.merchant_id
    LEFT JOIN dim_mcc dmcc
        ON stg.mcc = dmcc.mcc_code
    LEFT JOIN dim_ch dch
        ON stg.channel = dch.channel_code
    LEFT JOIN dim_err derr
        ON stg.error_code_clean = derr.error_code
    LEFT JOIN dim_dt dt
        ON stg.transaction_date = dt.full_date
)

SELECT * FROM joined