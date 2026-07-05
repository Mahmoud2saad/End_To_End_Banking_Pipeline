{{
    config(
        materialized='incremental',
        unique_key='wallet_transaction_key',
        incremental_strategy='merge',
        tags=['facts'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_txn_date ON {{ this }} (transaction_date)"
        ]
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_wallet_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

dim_atm AS (
    SELECT atm_key, atm_id, geography_key FROM {{ ref('dim_atm') }}
),

dim_ch AS (
    SELECT channel_key, channel_code FROM {{ ref('dim_channel') }}
),

dim_dt AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stg.transaction_id']) }}
                                                AS wallet_transaction_key,
        dt.date_key,
        da.atm_key,
        da.geography_key,
        dch.channel_key,
        stg.transaction_id,
        stg.mobile_number_masked,
        stg.amount_mad,
        stg.transaction_date,
        stg.transaction_hour,
        stg.transaction_type,
        stg.transaction_status,
        stg.is_reversal,
        stg.is_cash_out,
        stg.is_successful,
        stg.currency,
        stg._silver_loaded_at,
        CURRENT_TIMESTAMP                       AS _loaded_at,
        {{ audit_columns() }}
    FROM stg
    LEFT JOIN dim_atm da
        ON stg.atm_id = da.atm_id
    LEFT JOIN dim_ch dch
        ON stg.channel = dch.channel_code
    LEFT JOIN dim_dt dt
        ON stg.transaction_date = dt.full_date
)

SELECT * FROM joined
