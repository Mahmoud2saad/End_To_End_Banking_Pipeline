{{ config(materialized='table', tags=['dimensions']) }}

WITH atm_locations AS (
    SELECT DISTINCT
        region          AS region,
        'Morocco'         AS country,
        'ATM'           AS source_type
    FROM {{ ref('stg_atm_master') }}
    WHERE region IS NOT NULL
),

merchant_locations AS (
    SELECT DISTINCT
        merchant_region AS region,
        'Morocco'         AS country,
        'MERCHANT'      AS source_type
    FROM {{ ref('stg_transactions') }}
    WHERE merchant_region IS NOT NULL
),

all_locations AS (
    SELECT region, country FROM atm_locations
    UNION
    SELECT region, country FROM merchant_locations
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['region', 'country']) }} AS geography_key,
        region,
        country,
        CASE
            WHEN region IN (
                'Casablanca-Settat', 'Rabat-Salé-Kénitra',
                'Tanger-Tétouan-Al Hoceïma', 'Fès-Meknès'
            ) THEN 'NORTH'
            WHEN region IN (
                'Marrakech-Safi', 'Souss-Massa',
                'Drâa-Tafilalet', 'Guelmim-Oued Noun'
            ) THEN 'SOUTH'
            WHEN region IN (
                'Oriental', 'Béni Mellal-Khénifra'
            ) THEN 'EAST'
            ELSE 'OTHER'
        END                                                            AS macro_region,
        CASE
            WHEN region = 'Casablanca-Settat' THEN 'TIER_1'
            WHEN region IN (
                'Rabat-Salé-Kénitra', 'Tanger-Tétouan-Al Hoceïma',
                'Fès-Meknès', 'Marrakech-Safi'
            ) THEN 'TIER_2'
            ELSE 'TIER_3'
        END                                                            AS population_tier,
        TRUE                                                           AS is_atm_location,
        TRUE                                                           AS is_merchant_location
    FROM all_locations
)

SELECT * FROM enriched