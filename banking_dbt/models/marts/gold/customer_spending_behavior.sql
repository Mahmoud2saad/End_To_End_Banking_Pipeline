{{ config(materialized='table', tags=['gold']) }}

WITH txns AS (
    SELECT * FROM {{ ref('fact_card_transactions') }}
),

customers AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

mcc AS (
    SELECT merchant_category_key, category_group
    FROM {{ ref('dim_merchant_category') }}
),

-- spending stats per customer
spending_stats AS (
    SELECT
        t.customer_key,
        COUNT(*)                                     AS total_transactions,
        SUM(t.amount_abs)                            AS total_spend_mad,
        AVG(t.amount_abs)                            AS avg_spend_mad,
        MAX(t.amount_abs)                            AS max_spend_mad,
        MIN(t.amount_abs)                            AS min_spend_mad,
        SUM(CASE WHEN t.transaction_hour BETWEEN 6 AND 11
                 THEN 1 ELSE 0 END)                 AS morning_txns,
        SUM(CASE WHEN t.transaction_hour BETWEEN 12 AND 17
                 THEN 1 ELSE 0 END)                 AS afternoon_txns,
        SUM(CASE WHEN t.transaction_hour BETWEEN 18 AND 22
                 THEN 1 ELSE 0 END)                 AS evening_txns,
        SUM(CASE WHEN t.transaction_hour BETWEEN 23 AND 5
                 THEN 1 ELSE 0 END)                 AS night_txns,
        SUM(CASE WHEN t.is_online THEN 1 ELSE 0 END) AS online_txns,
        SUM(CASE WHEN t.is_fraud THEN 1 ELSE 0 END)   AS fraud_txns,
        COUNT(DISTINCT t.transaction_date)           AS active_days,
        COUNT(DISTINCT t.merchant_category_key)      AS unique_categories
    FROM txns t
    GROUP BY t.customer_key
),

-- top spending category per customer
top_category AS (
    SELECT
        t.customer_key,
        m.category_group                           AS preferred_category,
        SUM(t.amount_mad)                          AS category_spend,
        ROW_NUMBER() OVER (
            PARTITION BY t.customer_key
            ORDER BY SUM(t.amount_mad) DESC
        )                                          AS rn
    FROM txns t
    LEFT JOIN mcc m ON t.merchant_category_key = m.merchant_category_key
    GROUP BY t.customer_key, m.category_group
),

preferred AS (
    SELECT customer_key, preferred_category
    FROM top_category
    WHERE rn = 1
),

combined AS (
    SELECT
        c.customer_key,
        c.client_id,
        c.income_segment,
        c.credit_tier,
        c.age_group,
        c.gender,
        c.country,
        c.yearly_income_mad,
        c.credit_score,
        COALESCE(s.total_transactions, 0)           AS total_transactions,
        COALESCE(s.total_spend_mad, 0)              AS total_spend_mad,
        COALESCE(s.avg_spend_mad, 0)                AS avg_spend_mad,
        COALESCE(s.max_spend_mad, 0)                AS max_spend_mad,
        COALESCE(s.morning_txns, 0)                 AS morning_txns,
        COALESCE(s.afternoon_txns, 0)               AS afternoon_txns,
        COALESCE(s.evening_txns, 0)                 AS evening_txns,
        COALESCE(s.night_txns, 0)                   AS night_txns,
        COALESCE(s.online_txns, 0)                  AS online_txns,
        COALESCE(s.fraud_txns, 0)                   AS fraud_txns,
        COALESCE(s.active_days, 0)                  AS active_days,
        COALESCE(s.unique_categories, 0)            AS unique_categories,
        COALESCE(p.preferred_category, 'UNKNOWN')   AS preferred_category,
        ROUND(
            COALESCE(s.total_spend_mad, 0) * 100.0
            / NULLIF(c.yearly_income_mad, 0), 2
        )                                           AS spend_to_income_ratio,
        ROUND(
            COALESCE(s.online_txns, 0) * 100.0
            / NULLIF(s.total_transactions, 0), 2
        )                                           AS online_ratio_pct,
        CASE
            WHEN COALESCE(s.total_transactions, 0) = 0 THEN 'INACTIVE'
            WHEN s.active_days >= 20 THEN 'VERY_ACTIVE'
            WHEN s.active_days >= 10 THEN 'ACTIVE'
            WHEN s.active_days >= 3  THEN 'OCCASIONAL'
            ELSE 'RARE'
        END                                         AS activity_level,
        CURRENT_TIMESTAMP                           AS _loaded_at
    FROM customers c
    LEFT JOIN spending_stats s ON c.customer_key = s.customer_key
    LEFT JOIN preferred p      ON c.customer_key = p.customer_key
)

SELECT * FROM combined