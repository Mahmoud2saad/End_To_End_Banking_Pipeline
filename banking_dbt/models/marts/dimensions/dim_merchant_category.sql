{{ config(materialized='table', tags=['dimensions']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

distinct_mcc AS (
    SELECT DISTINCT mcc AS mcc_code
    FROM stg
    WHERE mcc IS NOT NULL
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['mcc_code']) }}    AS merchant_category_key,
        mcc_code,
        CASE
            WHEN mcc_code = '6011' THEN 'Automated Cash Disbursements'
            WHEN mcc_code = '5411' THEN 'Grocery Stores'
            WHEN mcc_code = '5812' THEN 'Eating Places & Restaurants'
            WHEN mcc_code = '5541' THEN 'Service Stations'
            WHEN mcc_code = '4111' THEN 'Transportation'
            WHEN mcc_code = '5310' THEN 'Discount Stores'
            WHEN mcc_code = '7011' THEN 'Hotels & Lodging'
            WHEN mcc_code = '5912' THEN 'Drug Stores & Pharmacies'
            WHEN mcc_code = '5045' THEN 'Electronics'
            WHEN mcc_code = '4814' THEN 'Telecommunication Services'
            ELSE CONCAT('MCC-', mcc_code)
        END                                                     AS mcc_description,
        CASE
            WHEN mcc_code IN ('6011','6012','6051') THEN 'Financial Services'
            WHEN mcc_code IN ('5411','5422','5812') THEN 'Food & Beverage'
            WHEN mcc_code IN ('5541','5983')        THEN 'Fuel'
            WHEN mcc_code IN ('4111','4131','4814') THEN 'Transport & Telecom'
            WHEN mcc_code IN ('5310','5311','5045') THEN 'Retail'
            WHEN mcc_code IN ('7011','7012')        THEN 'Hospitality'
            ELSE 'Other'
        END                                                     AS category_group,
        CASE
            WHEN mcc_code IN ('6011','7995','5933') THEN TRUE
            ELSE FALSE
        END                                                     AS is_high_risk_mcc
    FROM distinct_mcc
)

SELECT * FROM enriched