{{ config(materialized='view', tags=['staging']) }}

WITH raw AS (
    SELECT * FROM {{ source('silver', 'atm_master') }}
),

cleaned AS (
    SELECT
        terminal_id               AS atm_id,
        region                    AS region,
        atm_type                  AS atm_type,
        provider                  AS provider,
        location_type             AS location_type,
        installation_date         AS installation_date,
        CAST(cash_limit AS DOUBLE) AS cash_limit_mad,
        is_cash_deposit_enabled   AS is_cash_deposit_enabled,
        atm_age_years             AS atm_age_years,
        country                   AS country,
        bank_name                 AS bank_name,
        currency                  AS currency,
        _silver_loaded_at         AS _silver_loaded_at
    FROM raw
    WHERE terminal_id IS NOT NULL
)

SELECT *
FROM cleaned
QUALIFY ROW_NUMBER() OVER (PARTITION BY atm_id ORDER BY _silver_loaded_at DESC) = 1