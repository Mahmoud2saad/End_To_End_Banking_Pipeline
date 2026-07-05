-- Kaggle transactions can have negatives (returns/refunds)
-- Only ATM and wallet transactions should never be negative

SELECT
    'fact_atm_transactions'     AS source_table,
    refnum                      AS record_id,
    amount_mad                  AS failing_amount
FROM {{ ref('fact_atm_transactions') }}
WHERE amount_mad < 0

UNION ALL

SELECT
    'fact_wallet_transactions'  AS source_table,
    wallet_transaction_key      AS record_id,
    amount_mad                  AS failing_amount
FROM {{ ref('fact_wallet_transactions') }}
WHERE amount_mad < 0