version: 2

models:

  - name: atm_performance
    description: "Gold — daily ATM performance metrics graded A–D by success rate and OOC events"
    columns:
      - name: atm_key
        tests: [not_null]
      - name: atm_id
        tests: [not_null]
      - name: performance_grade
        tests:
          - not_null
          - accepted_values:
              values: ['A', 'B', 'C', 'D']
      - name: success_rate_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
      - name: capacity_tier
        tests:
          - accepted_values:
              values: ['LOW_CAPACITY', 'MEDIUM_CAPACITY', 'HIGH_CAPACITY']

  - name: fraud_risk_scoring
    description: "Gold — customer fraud risk scores (0–100) with risk level classification"
    columns:
      - name: customer_key
        tests: [not_null, unique]
      - name: client_id
        tests: [not_null, unique]
      - name: fraud_risk_score
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
      - name: risk_level
        tests:
          - not_null
          - accepted_values:
              values: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
      - name: fraud_rate_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100

  - name: customer_spending_behavior
    description: "Gold — customer spending patterns, channel preferences, and activity levels"
    columns:
      - name: customer_key
        tests: [not_null, unique]
      - name: client_id
        tests: [not_null, unique]
      - name: activity_level
        tests:
          - not_null
          - accepted_values:
              values: ['INACTIVE', 'RARE', 'OCCASIONAL', 'ACTIVE', 'VERY_ACTIVE']
      - name: total_spend_mad
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
      - name: online_ratio_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100

  - name: replenishment_analysis
    description: "Gold — daily ATM cash replenishment urgency and utilization analysis"
    columns:
      - name: atm_key
        tests: [not_null]
      - name: replenishment_urgency
        tests:
          - not_null
          - accepted_values:
              values: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
      - name: replenishment_status
        tests:
          - not_null
          - accepted_values:
              values: ['OK', 'SOON', 'URGENT']
      - name: cash_utilization_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0

  - name: channel_comparison
    description: "Gold — daily channel KPI comparison across ATM, Wallet, and Card transactions"
    columns:
      - name: channel_key
        tests: [not_null]
      - name: channel_code
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]
      - name: success_rate_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
      - name: time_of_day
        tests:
          - not_null
          - accepted_values:
              values: ['MORNING', 'AFTERNOON', 'EVENING', 'NIGHT']

  - name: governorate_summary
    description: "Gold — transaction volume and channel share summarised by Moroccan region"
    columns:
      - name: region
        tests: [not_null, unique]
      - name: country
        tests: [not_null]
      - name: macro_region
        tests:
          - not_null
          - accepted_values:
              values: ['NORTH', 'SOUTH', 'EAST', 'OTHER']
      - name: total_atms
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
      - name: atm_success_rate_pct
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100
