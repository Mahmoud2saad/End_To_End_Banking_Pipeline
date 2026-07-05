SELECT
    atm_transaction_key AS record_id,
    resp_code           AS failing_resp_code,
    'atm'               AS source
FROM {{ ref('fact_atm_transactions') }}
WHERE resp_code IS NOT NULL
  AND resp_code NOT IN (0, 14, 43, 51, 54, 57, 61, 91, 96)

UNION ALL

SELECT
    out_of_cash_key     AS record_id,
    resp_code           AS failing_resp_code,
    'ooc'               AS source
FROM {{ ref('fact_out_of_cash_events') }}
WHERE resp_code IS NOT NULL
  AND resp_code NOT IN (0, 14, 43, 51, 54, 57, 61, 91, 96)