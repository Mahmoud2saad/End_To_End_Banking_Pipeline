SELECT
    wallet_transaction_key AS record_id,
    amount_mad             AS failing_amount
FROM {{ ref('fact_wallet_transactions') }}
WHERE amount_mad <= 0