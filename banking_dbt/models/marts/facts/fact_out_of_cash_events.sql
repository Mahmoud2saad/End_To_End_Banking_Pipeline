--- Meds is going to  craete the fact table for out of cash events and this is the first trail for incremental load of fact table out of cash events.
-- but it's kinda complicated because we have to join with dim_error_type to get the error_type_key and also we have to join with dim_atm to get the atm_key and also we have to join with dim_date to get the date_key.
{{ config(
    materialized='incremental',
    unique_key='out_of_cash_key',
    incremental_strategy='merge',
    tags=['facts']
) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_out_of_cash') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

dim_atm AS (
    SELECT atm_key, atm_id FROM {{ ref('dim_atm') }}
),

dim_err AS (
    SELECT error_type_key, CAST(error_code AS VARCHAR) AS error_code
    FROM {{ ref('dim_error_type') }}
),

dim_dt AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stg.refnum']) }}
                                                AS out_of_cash_key,
        dt.date_key,
        da.atm_key,
        derr.error_type_key,
        stg.refnum,
        stg.pan_masked,
        stg.attempted_amount_mad,
        stg.transaction_date,
        stg.transaction_hour,
        stg.resp_code,
        stg.failure_reason,
        stg.is_confirmed_ooc,
        stg.currency,
        stg._silver_loaded_at,
        CURRENT_TIMESTAMP                       AS _loaded_at
    FROM stg
    LEFT JOIN dim_atm da
        ON stg.atm_id = da.atm_id
    LEFT JOIN dim_err derr
        ON CAST(stg.resp_code AS VARCHAR) = derr.error_code
    LEFT JOIN dim_dt dt
        ON stg.transaction_date = dt.full_date
)

SELECT * FROM joined