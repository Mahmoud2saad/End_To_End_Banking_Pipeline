{{ config(materialized='table', tags=['gold']) }}

WITH card_txns AS (
    SELECT * FROM {{ ref('fact_card_transactions') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

cards AS (
    SELECT * FROM {{ ref('dim_card') }}
),

-- transaction stats per customer
txn_stats AS (
    SELECT
        customer_key,
        COUNT(*)                                        AS total_transactions,
        SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END)      AS fraud_transactions,
        SUM(amount_mad)                                 AS total_amount_mad,
        AVG(amount_mad)                                 AS avg_amount_mad,
        MAX(amount_mad)                                 AS max_amount_mad,
        ROUND(
            SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 4
        )                                               AS fraud_rate_pct,
        SUM(CASE WHEN is_online THEN 1 ELSE 0 END)     AS online_transactions,
        COUNT(DISTINCT transaction_date)                AS active_days
    FROM card_txns
    GROUP BY customer_key
),

-- card risk per customer
card_risk AS (
    SELECT
        customer_key,
        COUNT(*)                                            AS total_cards,
        SUM(CASE WHEN card_on_dark_web THEN 1 ELSE 0 END) AS dark_web_cards,
        SUM(CASE WHEN is_expired THEN 1 ELSE 0 END)       AS expired_cards,
        MAX(credit_limit_mad)                              AS max_credit_limit
    FROM cards
    GROUP BY customer_key
),

combined AS (
    SELECT
        c.customer_key,
        c.client_id,
        c.income_segment,
        c.credit_tier,
        c.debt_risk_level,
        c.age_group,
        c.gender,
        c.country,
        COALESCE(t.total_transactions, 0)   AS total_transactions,
        COALESCE(t.fraud_transactions, 0)   AS fraud_transactions,
        COALESCE(t.total_amount_mad, 0)     AS total_amount_mad,
        COALESCE(t.avg_amount_mad, 0)       AS avg_amount_mad,
        COALESCE(t.max_amount_mad, 0)       AS max_amount_mad,
        COALESCE(t.fraud_rate_pct, 0)       AS fraud_rate_pct,
        COALESCE(t.online_transactions, 0)  AS online_transactions,
        COALESCE(t.active_days, 0)          AS active_days,
        COALESCE(cr.total_cards, 0)         AS total_cards,
        COALESCE(cr.dark_web_cards, 0)      AS dark_web_cards,
        COALESCE(cr.expired_cards, 0)       AS expired_cards,
        COALESCE(cr.max_credit_limit, 0)    AS max_credit_limit_mad
    FROM customers c
    LEFT JOIN txn_stats t     ON c.customer_key = t.customer_key
    LEFT JOIN card_risk cr    ON c.customer_key = cr.customer_key
),

scored AS (
    SELECT
        *,
        -- fraud risk score 0-100
        LEAST(100, ROUND(
            (fraud_rate_pct * 40)
            + (CASE WHEN dark_web_cards > 0 THEN 30 ELSE 0 END)
            + (CASE WHEN debt_risk_level = 'HIGH_RISK'   THEN 15 ELSE 0 END)
            + (CASE WHEN credit_tier = 'POOR'            THEN 10 ELSE 0 END)
            + (CASE WHEN max_amount_mad > {{ var('fraud_amount_threshold') }}
                    THEN 5 ELSE 0 END)
        , 2))                           AS fraud_risk_score,
        CASE
            WHEN dark_web_cards > 0           THEN 'CRITICAL'
            WHEN fraud_rate_pct > 10          THEN 'HIGH'
            WHEN fraud_rate_pct > 5
              OR debt_risk_level = 'HIGH_RISK' THEN 'MEDIUM'
            ELSE 'LOW'
        END                             AS risk_level,
        CURRENT_TIMESTAMP               AS _loaded_at
    FROM combined
)

SELECT * FROM scored