version: 2

models:

  - name: stg_atm_master
    description: "Staging view — 159 Moroccan ATMs from silver atm_master"
    columns:
      - name: atm_id
        description: "Unique ATM identifier (terminal_id from silver)"
        tests: [not_null, unique]
      - name: region
        tests: [not_null]
      - name: cash_limit_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0

  - name: stg_atm_transactions
    description: "Staging view — ATM card transactions (withdrawal/deposit) from silver card_transactions"
    columns:
      - name: refnum
        description: "Transaction reference number"
        tests: [not_null, unique]
      - name: atm_id
        tests: [not_null]
      - name: amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_successful
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]

  - name: stg_card_transactions
    description: "Staging view — all card channel transactions from silver card_transactions"
    columns:
      - name: refnum
        tests: [not_null, unique]
      - name: client_id
        tests: [not_null]
      - name: amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: transaction_date
        tests: [not_null]

  - name: stg_cards
    description: "Staging view — 6,146 bank cards from silver cards"
    columns:
      - name: card_id
        tests: [not_null, unique]
      - name: client_id
        tests: [not_null]
      - name: dark_web_risk
        tests:
          - accepted_values:
              values: ['LOW', 'HIGH']

  - name: stg_customers
    description: "Staging view — 2,000 Moroccan banking customers from silver customers"
    columns:
      - name: client_id
        tests: [not_null, unique]
      - name: credit_score
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 300
              max_value: 850
      - name: gender
        tests:
          - accepted_values:
              values: ['M', 'F', 'MALE', 'FEMALE', 'OTHER']
      - name: country
        tests: [not_null]

  - name: stg_out_of_cash
    description: "Staging view — ATM out-of-cash failure events from silver out_of_cash"
    columns:
      - name: refnum
        tests: [not_null]
      - name: atm_id
        tests: [not_null]
      - name: attempted_amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_confirmed_ooc
        tests: [not_null]

  - name: stg_transactions
    description: "Staging view — Kaggle financial transactions with fraud labels from silver kaggle_transactions"
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: client_id
        tests: [not_null]
      - name: card_id
        tests: [not_null]
      - name: amount_mad
        tests: [not_null]
      - name: is_fraud
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]

  - name: stg_wallet_transactions
    description: "Staging view — mobile wallet transactions from silver wallet_transactions"
    columns:
      - name: transaction_id
        tests: [not_null, unique]
      - name: amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_successful
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]
