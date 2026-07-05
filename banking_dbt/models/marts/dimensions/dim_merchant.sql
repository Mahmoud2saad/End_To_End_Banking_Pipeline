{{ config(materialized='table', tags=['dimensions']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

ranked_merchants AS (
    SELECT
        merchant_id,
        merchant_city,
        merchant_region,
        zip,
        mcc,
        ROW_NUMBER() OVER (
            PARTITION BY merchant_id
            ORDER BY merchant_city, merchant_region, zip, mcc
        ) AS rn
    FROM stg
    WHERE merchant_id IS NOT NULL
),

distinct_merchants AS (
    SELECT
        merchant_id,
        merchant_city,
        merchant_region,
        zip,
        mcc
    FROM ranked_merchants
    WHERE rn = 1
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['merchant_id']) }} AS merchant_key,
        merchant_id,
        merchant_city,
        merchant_region,
        zip,
        mcc AS mcc_code,
        CASE
            WHEN mcc = '6011' THEN TRUE
            ELSE FALSE
        END AS is_atm_merchant,
        CASE
            WHEN mcc IN ('5411','5412','5422') THEN 'GROCERY'
            WHEN mcc IN ('5812','5813','5814') THEN 'FOOD_BEVERAGE'
            WHEN mcc IN ('5541','5542','5983') THEN 'FUEL'
            WHEN mcc IN ('4111','4112','4131') THEN 'TRANSPORT'
            WHEN mcc IN ('6011','6012') THEN 'FINANCIAL'
            WHEN mcc IN ('5310','5311','5331') THEN 'RETAIL'
            WHEN mcc IN ('7011','7012') THEN 'HOSPITALITY'
            ELSE 'OTHER'
        END AS merchant_category_group,
        'Morocco' AS country
    FROM distinct_merchants
)

SELECT * FROM enriched