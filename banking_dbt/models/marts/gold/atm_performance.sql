{{ config(materialized='table', tags=['gold']) }}

WITH atm_txns AS (
    SELECT * FROM {{ ref('fact_atm_transactions') }}
),

ooc AS (
    SELECT * FROM {{ ref('fact_out_of_cash_events') }}
),

wallet AS (
    SELECT * FROM {{ ref('fact_wallet_transactions') }}
),

dim_atm AS (
    SELECT * FROM {{ ref('dim_atm') }}
),

atm_card_stats AS (
    SELECT
        atm_key,
        transaction_date,
        COUNT(*) AS total_transactions,
        SUM(CASE WHEN is_successful THEN 1 ELSE 0 END) AS successful_transactions,
        SUM(CASE WHEN is_reversal THEN 1 ELSE 0 END) AS reversals,
        SUM(CASE WHEN is_deposit THEN 1 ELSE 0 END) AS deposits,
        SUM(amount_mad) AS total_amount_mad,
        AVG(amount_mad) AS avg_transaction_mad,
        COUNT(DISTINCT transaction_hour) AS active_hours
    FROM atm_txns
    GROUP BY atm_key, transaction_date
),

ooc_stats AS (
    SELECT
        atm_key,
        transaction_date,
        COUNT(*) AS ooc_events,
        SUM(attempted_amount_mad) AS total_attempted_mad
    FROM ooc
    GROUP BY atm_key, transaction_date
),

wallet_stats AS (
    SELECT
        atm_key,
        transaction_date,
        COUNT(*) AS wallet_transactions,
        SUM(amount_mad) AS wallet_amount_mad
    FROM wallet
    GROUP BY atm_key, transaction_date
),

combined AS (
    SELECT
        d.atm_key,
        d.atm_id,
        d.region,
        d.atm_type,
        d.provider,
        d.cash_limit_mad,
        d.is_cash_deposit_enabled,
        d.capacity_tier,
        COALESCE(c.transaction_date, o.transaction_date, w.transaction_date) AS transaction_date,
        COALESCE(c.total_transactions, 0) AS card_transactions,
        COALESCE(c.successful_transactions, 0) AS successful_card_txns,
        COALESCE(c.reversals, 0) AS reversals,
        COALESCE(c.deposits, 0) AS deposits,
        COALESCE(c.total_amount_mad, 0) AS card_amount_mad,
        COALESCE(c.avg_transaction_mad, 0) AS avg_card_amount_mad,
        COALESCE(o.ooc_events, 0) AS ooc_events,
        COALESCE(o.total_attempted_mad, 0) AS attempted_amount_mad,
        COALESCE(w.wallet_transactions, 0) AS wallet_transactions,
        COALESCE(w.wallet_amount_mad, 0) AS wallet_amount_mad
    FROM dim_atm d
    LEFT JOIN atm_card_stats c
        ON d.atm_key = c.atm_key
    LEFT JOIN ooc_stats o
        ON d.atm_key = o.atm_key
       AND c.transaction_date = o.transaction_date
    LEFT JOIN wallet_stats w
        ON d.atm_key = w.atm_key
       AND c.transaction_date = w.transaction_date
),

graded AS (
    SELECT
        *,
        ROUND(successful_card_txns * 100.0 / NULLIF(card_transactions, 0), 2) AS success_rate_pct,
        card_transactions + wallet_transactions AS total_transactions,
        card_amount_mad + wallet_amount_mad AS total_amount_mad,
        CASE
            WHEN ooc_events > 5 THEN 'D'
            WHEN successful_card_txns * 100.0 / NULLIF(card_transactions, 0) >= 95 THEN 'A'
            WHEN successful_card_txns * 100.0 / NULLIF(card_transactions, 0) >= 85 THEN 'B'
            WHEN successful_card_txns * 100.0 / NULLIF(card_transactions, 0) >= 70 THEN 'C'
            ELSE 'D'
        END AS performance_grade,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM combined
)

SELECT * FROM graded