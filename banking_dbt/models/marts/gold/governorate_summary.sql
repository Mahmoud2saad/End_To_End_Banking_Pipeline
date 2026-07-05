{{ config(materialized='table', tags=['gold']) }}

WITH geo AS (
    SELECT * FROM {{ ref('dim_geography') }}
),

atm_dim AS (
    SELECT atm_key, atm_id, region FROM {{ ref('dim_atm') }}
),

atm_txns AS (
    SELECT
        da.region,
        COUNT(*)                                            AS atm_transactions,
        SUM(f.amount_mad)                                   AS atm_amount_mad,
        SUM(CASE WHEN f.is_successful THEN 1 ELSE 0 END)   AS successful_atm_txns,
        SUM(CASE WHEN f.is_reversal THEN 1 ELSE 0 END)     AS atm_reversals
    FROM {{ ref('fact_atm_transactions') }} f
    LEFT JOIN atm_dim da ON f.atm_key = da.atm_key
    WHERE da.region IS NOT NULL
    GROUP BY da.region
),

wallet_txns AS (
    SELECT
        da.region,
        COUNT(*)                                            AS wallet_transactions,
        SUM(f.amount_mad)                                   AS wallet_amount_mad,
        SUM(CASE WHEN f.is_successful THEN 1 ELSE 0 END)   AS successful_wallet_txns
    FROM {{ ref('fact_wallet_transactions') }} f
    LEFT JOIN atm_dim da ON f.atm_key = da.atm_key
    WHERE da.region IS NOT NULL
    GROUP BY da.region
),

ooc_events AS (
    SELECT
        da.region,
        COUNT(*)                                            AS ooc_events,
        SUM(f.attempted_amount_mad)                         AS attempted_amount_mad
    FROM {{ ref('fact_out_of_cash_events') }} f
    LEFT JOIN atm_dim da ON f.atm_key = da.atm_key
    WHERE da.region IS NOT NULL
    GROUP BY da.region
),

atm_count AS (
    SELECT region, COUNT(*) AS total_atms
    FROM atm_dim
    GROUP BY region
),

combined AS (
    SELECT
        g.region,
        g.country,
        g.macro_region,
        g.population_tier,
        COALESCE(ac.total_atms, 0)              AS total_atms,
        COALESCE(a.atm_transactions, 0)         AS atm_transactions,
        COALESCE(a.atm_amount_mad, 0)           AS atm_amount_mad,
        COALESCE(a.successful_atm_txns, 0)      AS successful_atm_txns,
        COALESCE(a.atm_reversals, 0)            AS atm_reversals,
        COALESCE(w.wallet_transactions, 0)      AS wallet_transactions,
        COALESCE(w.wallet_amount_mad, 0)        AS wallet_amount_mad,
        COALESCE(w.successful_wallet_txns, 0)   AS successful_wallet_txns,
        COALESCE(o.ooc_events, 0)               AS ooc_events,
        COALESCE(o.attempted_amount_mad, 0)     AS ooc_attempted_mad
    FROM geo g
    LEFT JOIN atm_count ac  ON g.region = ac.region
    LEFT JOIN atm_txns a    ON g.region = a.region
    LEFT JOIN wallet_txns w ON g.region = w.region
    LEFT JOIN ooc_events o  ON g.region = o.region
),

enriched AS (
    SELECT
        *,
        atm_transactions + wallet_transactions          AS total_transactions,
        atm_amount_mad + wallet_amount_mad              AS total_amount_mad,
        ROUND(
            successful_atm_txns * 100.0
            / NULLIF(atm_transactions, 0), 2
        )                                               AS atm_success_rate_pct,
        ROUND(
            ooc_events * 100.0
            / NULLIF(atm_transactions + ooc_events, 0), 2
        )                                               AS ooc_rate_pct,
        ROUND(
            wallet_transactions * 100.0
            / NULLIF(atm_transactions + wallet_transactions, 0), 2
        )                                               AS wallet_share_pct,
        CURRENT_TIMESTAMP                               AS _loaded_at
    FROM combined
)

SELECT * FROM enriched
ORDER BY total_transactions DESC