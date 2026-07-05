{{ config(materialized='table', tags=['gold']) }}

WITH dim_atm AS (
    SELECT * FROM {{ ref('dim_atm') }}
),

ooc AS (
    SELECT
        atm_key,
        transaction_date,
        COUNT(*)                            AS ooc_events,
        SUM(attempted_amount_mad)           AS total_attempted_mad,
        MIN(transaction_hour)               AS first_ooc_hour
    FROM {{ ref('fact_out_of_cash_events') }}
    GROUP BY atm_key, transaction_date
),

atm_txns AS (
    SELECT
        atm_key,
        transaction_date,
        SUM(amount_mad)                     AS total_dispensed_mad,
        COUNT(*)                            AS transactions
    FROM {{ ref('fact_atm_transactions') }}
    WHERE is_successful = TRUE
      AND is_deposit = FALSE
    GROUP BY atm_key, transaction_date
),

daily_stats AS (
    SELECT
        d.atm_key,
        d.atm_id,
        d.region,
        d.atm_type,
        d.provider,
        d.cash_limit_mad,
        d.capacity_tier,
        COALESCE(t.transaction_date, o.transaction_date)
                                            AS activity_date,
        COALESCE(t.total_dispensed_mad, 0)  AS dispensed_mad,
        COALESCE(t.transactions, 0)         AS card_transactions,
        COALESCE(o.ooc_events, 0)           AS ooc_events,
        COALESCE(o.total_attempted_mad, 0)  AS attempted_after_empty_mad,
        COALESCE(o.first_ooc_hour, NULL)    AS first_ooc_hour
    FROM dim_atm d
    LEFT JOIN atm_txns t ON d.atm_key = t.atm_key
    LEFT JOIN ooc o      ON d.atm_key = o.atm_key
                        AND t.transaction_date = o.transaction_date
),

enriched AS (
    SELECT
        *,
        ROUND(
            dispensed_mad * 100.0
            / NULLIF(cash_limit_mad, 0), 2
        )                                   AS cash_utilization_pct,
        CASE
            WHEN ooc_events > 0             THEN TRUE
            ELSE FALSE
        END                                 AS ran_out_of_cash,
        CASE
            WHEN ooc_events > 3             THEN 'CRITICAL'
            WHEN ooc_events > 1             THEN 'HIGH'
            WHEN ooc_events = 1             THEN 'MEDIUM'
            ELSE 'LOW'
        END                                 AS replenishment_urgency,
        CASE
            WHEN dispensed_mad * 100.0
                / NULLIF(cash_limit_mad, 0) > 80 THEN 'URGENT'
            WHEN dispensed_mad * 100.0
                / NULLIF(cash_limit_mad, 0) > 60 THEN 'SOON'
            ELSE 'OK'
        END                                 AS replenishment_status,
        CURRENT_TIMESTAMP                   AS _loaded_at
    FROM daily_stats
)

SELECT * FROM enriched
ORDER BY ooc_events DESC, cash_utilization_pct DESC