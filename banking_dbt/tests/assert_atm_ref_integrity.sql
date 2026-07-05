SELECT
    f.atm_transaction_key AS record_id,
    f.atm_key             AS failing_atm_key
FROM {{ ref('fact_atm_transactions') }} f
LEFT JOIN {{ ref('dim_atm') }} d
    ON f.atm_key = d.atm_key
WHERE f.atm_key IS NOT NULL
  AND d.atm_key IS NULL