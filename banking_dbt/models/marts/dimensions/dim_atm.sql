{{ config(materialized='table', tags=['dimensions']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_atm_master') }}
),

geo AS (
    SELECT geography_key, region, country
    FROM {{ ref('dim_geography') }}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['atm_id']) }}  AS atm_key,
        atm_id,
        stg.region,
        geo.geography_key,
        atm_type,
        provider,
        location_type,
        installation_date,
        cash_limit_mad,
        is_cash_deposit_enabled,
        atm_age_years,
        CASE
            WHEN atm_age_years <= 2  THEN 'NEW'
            WHEN atm_age_years <= 5  THEN 'ESTABLISHED'
            ELSE 'MATURE'
        END                                                 AS atm_maturity,
        CASE
            WHEN cash_limit_mad >= 1000000 THEN 'HIGH_CAPACITY'
            WHEN cash_limit_mad >= 800000  THEN 'MEDIUM_CAPACITY'
            ELSE 'LOW_CAPACITY'
        END                                                 AS capacity_tier,
        stg.country,
        bank_name,
        currency,
        _silver_loaded_at,
        {{ audit_columns() }}
    FROM stg
    LEFT JOIN geo
        ON stg.region = geo.region
        -- Joining on region only, not region+country: atm_master.csv has no
        -- country column at all (it's added during Bronze->Silver), and
        -- dim_geography hardcodes country='Morocco' while other parts of
        -- this codebase use 'Maroc' (config.py's COUNTRY constant). Since
        -- this is a single-country dataset, region alone is the real
        -- matching key -- adding country risked a silent all-NULL
        -- geography_key if the two literals never actually matched.
)

SELECT * FROM enriched
