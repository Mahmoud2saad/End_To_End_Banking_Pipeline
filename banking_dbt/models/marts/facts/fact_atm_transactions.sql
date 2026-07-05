{{
    config(
        materialized='incremental',
        unique_key='atm_transaction_key',
        incremental_strategy='merge',
        tags=['facts'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_txn_date ON {{ this }} (transaction_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_client_id ON {{ this }} (client_id)"
        ]
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_atm_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

dim_cust AS (
    SELECT customer_key, client_id
    FROM {{ ref('dim_customer') }}
),

dim_atm AS (
    SELECT atm_key, atm_id, geography_key FROM {{ ref('dim_atm') }}
),

dim_ch AS (
    SELECT channel_key, channel_code FROM {{ ref('dim_channel') }}
),

dim_err AS (
    SELECT error_type_key, CAST(error_code AS VARCHAR) AS error_code
    FROM {{ ref('dim_error_type') }}
),

dim_dt AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

-- Real relationship, not a fabricated one: pan_customer_map and
-- stg_atm_transactions both derive from the same Moroccan client_id
-- universe (unlike fact_card_transactions, which sources from the
-- unrelated Kaggle dataset -- see models/marts/facts/schema.yml for that
-- documented decision not to join PAN there).
--
-- Deduplicated to exactly one pan_key per client_id before joining: a
-- customer can legitimately have multiple PANs/cards in the source data,
-- and joining fact_atm_transactions to pan_customer_map on client_id
-- WITHOUT this dedup would silently fan out the fact table (one row per
-- matching PAN instead of one row per transaction) -- exactly the kind of
-- grain-breaking bug this whole change is meant to prevent, not cause.
dim_pan AS (
    SELECT client_id, pan_key
    FROM (
        SELECT
            client_id,
            pan_key,
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY pan_key) AS rn
        FROM {{ ref('pan_customer_map') }}
    ) ranked
    WHERE rn = 1
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stg.refnum']) }} AS atm_transaction_key,
        dt.date_key,
        dc.customer_key,
        CAST(NULL AS VARCHAR) AS card_key,
        da.atm_key,
        da.geography_key,
        dp.pan_key,
        dch.channel_key,
        derr.error_type_key,
        stg.refnum,
        stg.amount_mad,
        stg.transaction_date,
        stg.transaction_hour,
        stg.transaction_type,
        stg.msg_type,
        stg.resp_code,
        stg.is_successful,
        stg.is_reversal,
        stg.is_out_of_cash,
        stg.is_deposit,
        stg.client_id,
        stg.currency,
        stg._silver_loaded_at,
        CURRENT_TIMESTAMP AS _loaded_at,
        {{ audit_columns() }}
    FROM stg
    LEFT JOIN dim_cust dc
        ON stg.client_id = dc.client_id
    LEFT JOIN dim_atm da
        ON stg.atm_id = da.atm_id
    LEFT JOIN dim_ch dch
        ON stg.channel = dch.channel_code
    LEFT JOIN dim_err derr
        ON CAST(stg.resp_code AS VARCHAR) = derr.error_code
    LEFT JOIN dim_dt dt
        ON stg.transaction_date = dt.full_date
    LEFT JOIN dim_pan dp
        ON stg.client_id = dp.client_id
)

SELECT * FROM joined
