{{ config(materialized='table', tags=['gold']) }}

WITH dim_channel AS (
    SELECT
        channel_key,
        channel_code
    FROM {{ ref('dim_channel') }}
),

atm_txns AS (
    SELECT
        channel_key,
        transaction_date,
        transaction_hour,
        COUNT(*) AS transactions,
        SUM(amount_mad) AS total_amount_mad,
        AVG(amount_mad) AS avg_amount_mad,
        COUNT(*) AS successful_txns,
        SUM(CASE WHEN is_reversal THEN 1 ELSE 0 END) AS reversals,
        SUM(CASE WHEN is_deposit THEN 1 ELSE 0 END) AS deposits
    FROM {{ ref('fact_atm_transactions') }}
    GROUP BY channel_key, transaction_date, transaction_hour
),

wallet_txns AS (
    SELECT
        channel_key,
        transaction_date,
        transaction_hour,
        COUNT(*) AS transactions,
        SUM(amount_mad) AS total_amount_mad,
        AVG(amount_mad) AS avg_amount_mad,
        COUNT(*) AS successful_txns,
        SUM(CASE WHEN is_reversal THEN 1 ELSE 0 END) AS reversals,
        0 AS deposits
    FROM {{ ref('fact_wallet_transactions') }}
    GROUP BY channel_key, transaction_date, transaction_hour
),

card_txns AS (
    SELECT
        channel_key,
        transaction_date,
        transaction_hour,
        COUNT(*) AS transactions,
        SUM(amount_mad) AS total_amount_mad,
        AVG(amount_mad) AS avg_amount_mad,
        COUNT(*) AS successful_txns,
        0 AS reversals,
        0 AS deposits
    FROM {{ ref('fact_card_transactions') }}
    GROUP BY channel_key, transaction_date, transaction_hour
),

combined AS (
    SELECT * FROM atm_txns
    UNION ALL
    SELECT * FROM wallet_txns
    UNION ALL
    SELECT * FROM card_txns
),

enriched AS (
    SELECT
        c.channel_key,
        d.channel_code,
        c.transaction_date,
        c.transaction_hour,
        c.transactions,
        c.total_amount_mad,
        c.avg_amount_mad,
        c.successful_txns,
        c.reversals,
        c.deposits,
        ROUND(c.successful_txns * 100.0 / NULLIF(c.transactions, 0), 2) AS success_rate_pct,
        CASE
            WHEN c.transaction_hour BETWEEN 6 AND 11 THEN 'MORNING'
            WHEN c.transaction_hour BETWEEN 12 AND 17 THEN 'AFTERNOON'
            WHEN c.transaction_hour BETWEEN 18 AND 22 THEN 'EVENING'
            ELSE 'NIGHT'
        END AS time_of_day,
        CURRENT_TIMESTAMP AS _loaded_at
    FROM combined c
    LEFT JOIN dim_channel d
        ON c.channel_key = d.channel_key
)

SELECT *
FROM enriched
ORDER BY transaction_date, channel_code, transaction_hour