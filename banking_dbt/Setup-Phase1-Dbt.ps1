<#
.SYNOPSIS
    Phase 1 dbt: wires geography_key and pan_key into the fact grain for
    real (not just building the dimensions and leaving them unused), adds
    relationships tests across every FK in facts/dimensions schema.yml,
    adds an audit_columns() macro, and adds indexes on hot filter columns.

.DESCRIPTION
    - macros/audit_columns.sql: new, consistent audit trail macro
    - models/marts/dimensions/dim_atm.sql: adds geography_key (joined on
      region only -- see in-file comment on why country is NOT used in
      the join)
    - models/marts/facts/fact_atm_transactions.sql: adds geography_key AND
      pan_key (deduplicated to prevent fan-out), plus post-hook indexes
    - models/marts/facts/fact_wallet_transactions.sql: adds geography_key,
      plus a post-hook index
    - models/marts/facts/schema.yml, models/marts/dimensions/schema.yml:
      relationships tests wired to every FK, natural-key uniqueness tests
      added alongside surrogate-key tests (a real grain proof, not just a
      hash-uniqueness proof)

    IMPORTANT: fact_card_transactions is deliberately NOT given a pan_key.
    It's built from the Kaggle dataset (a different synthetic universe
    than pan_customer_map's Moroccan client_id space) -- joining them would
    fabricate a relationship that looks real but isn't. This is documented
    in the new schema.yml, not silently skipped.

    Run this from your Banking_dbt/ folder (or point -DbtProjectDir at it).
    Overwrites the 4 SQL/macro files it touches; the two schema.yml files
    are also full overwrites (they already existed with content -- this
    replaces them with the extended version, preserving every existing
    test and adding the new ones).

.EXAMPLE
    cd "D:\NTI INTERNSHIP\Airflow\Banking_pipeline\Banking_dbt"
    .\Setup-Phase1-Dbt.ps1
#>

[CmdletBinding()]
param(
    [string]$DbtProjectDir = "."
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$Message)
    Write-Host "[Setup-Phase1-Dbt] $Message"
}

function Write-FileForce {
    param([string]$Path, [string]$Content)
    $fullPath = Join-Path $DbtProjectDir $Path
    $dir = Split-Path $fullPath -Parent
    if ($dir -and -not (Test-Path $dir)) {
        New-Item -ItemType Directory -Force -Path $dir | Out-Null
    }
    $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText($fullPath, $Content, $utf8NoBom)
    Write-Log "Wrote: $fullPath"
}

$auditMacro = @'
{#
    macros/audit_columns.sql

    Returns a consistent set of audit columns to append to any mart model,
    so every row is traceable back to the exact dbt run that produced it —
    see docs/SECURITY_AND_GOVERNANCE.md and the audit trail design from
    Phase 1 planning.

    Usage in a model's final SELECT:
        SELECT
            ...,
            {{ audit_columns() }}
        FROM joined
#}
{% macro audit_columns() %}
    '{{ invocation_id }}'   AS _dbt_invocation_id,
    CURRENT_TIMESTAMP       AS _dbt_run_at
{% endmacro %}

'@
$dimAtm = @'
{{ config(materialized='table', tags=['dimensions']) }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_atm_master') }}
),

geo AS (
    SELECT geography_key, region, country
    FROM {{ ref('dim_geography') }}
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['atm_id']) }}  AS atm_key,
        atm_id,
        stg.region,
        geo.geography_key,
        atm_type,
        provider,
        location_type,
        installation_date,
        cash_limit_mad,
        is_cash_deposit_enabled,
        atm_age_years,
        CASE
            WHEN atm_age_years <= 2  THEN 'NEW'
            WHEN atm_age_years <= 5  THEN 'ESTABLISHED'
            ELSE 'MATURE'
        END                                                 AS atm_maturity,
        CASE
            WHEN cash_limit_mad >= 1000000 THEN 'HIGH_CAPACITY'
            WHEN cash_limit_mad >= 800000  THEN 'MEDIUM_CAPACITY'
            ELSE 'LOW_CAPACITY'
        END                                                 AS capacity_tier,
        stg.country,
        bank_name,
        currency,
        _silver_loaded_at,
        {{ audit_columns() }}
    FROM stg
    LEFT JOIN geo
        ON stg.region = geo.region
        -- Joining on region only, not region+country: atm_master.csv has no
        -- country column at all (it's added during Bronze->Silver), and
        -- dim_geography hardcodes country='Morocco' while other parts of
        -- this codebase use 'Maroc' (config.py's COUNTRY constant). Since
        -- this is a single-country dataset, region alone is the real
        -- matching key -- adding country risked a silent all-NULL
        -- geography_key if the two literals never actually matched.
)

SELECT * FROM enriched

'@
$factAtm = @'
{{
    config(
        materialized='incremental',
        unique_key='atm_transaction_key',
        incremental_strategy='merge',
        tags=['facts'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_txn_date ON {{ this }} (transaction_date)",
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_client_id ON {{ this }} (client_id)"
        ]
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_atm_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

dim_cust AS (
    SELECT customer_key, client_id
    FROM {{ ref('dim_customer') }}
),

dim_atm AS (
    SELECT atm_key, atm_id, geography_key FROM {{ ref('dim_atm') }}
),

dim_ch AS (
    SELECT channel_key, channel_code FROM {{ ref('dim_channel') }}
),

dim_err AS (
    SELECT error_type_key, CAST(error_code AS VARCHAR) AS error_code
    FROM {{ ref('dim_error_type') }}
),

dim_dt AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

-- Real relationship, not a fabricated one: pan_customer_map and
-- stg_atm_transactions both derive from the same Moroccan client_id
-- universe (unlike fact_card_transactions, which sources from the
-- unrelated Kaggle dataset -- see models/marts/facts/schema.yml for that
-- documented decision not to join PAN there).
--
-- Deduplicated to exactly one pan_key per client_id before joining: a
-- customer can legitimately have multiple PANs/cards in the source data,
-- and joining fact_atm_transactions to pan_customer_map on client_id
-- WITHOUT this dedup would silently fan out the fact table (one row per
-- matching PAN instead of one row per transaction) -- exactly the kind of
-- grain-breaking bug this whole change is meant to prevent, not cause.
dim_pan AS (
    SELECT client_id, pan_key
    FROM (
        SELECT
            client_id,
            pan_key,
            ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY pan_key) AS rn
        FROM {{ ref('pan_customer_map') }}
    ) ranked
    WHERE rn = 1
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stg.refnum']) }} AS atm_transaction_key,
        dt.date_key,
        dc.customer_key,
        CAST(NULL AS VARCHAR) AS card_key,
        da.atm_key,
        da.geography_key,
        dp.pan_key,
        dch.channel_key,
        derr.error_type_key,
        stg.refnum,
        stg.amount_mad,
        stg.transaction_date,
        stg.transaction_hour,
        stg.transaction_type,
        stg.msg_type,
        stg.resp_code,
        stg.is_successful,
        stg.is_reversal,
        stg.is_out_of_cash,
        stg.is_deposit,
        stg.client_id,
        stg.currency,
        stg._silver_loaded_at,
        CURRENT_TIMESTAMP AS _loaded_at,
        {{ audit_columns() }}
    FROM stg
    LEFT JOIN dim_cust dc
        ON stg.client_id = dc.client_id
    LEFT JOIN dim_atm da
        ON stg.atm_id = da.atm_id
    LEFT JOIN dim_ch dch
        ON stg.channel = dch.channel_code
    LEFT JOIN dim_err derr
        ON CAST(stg.resp_code AS VARCHAR) = derr.error_code
    LEFT JOIN dim_dt dt
        ON stg.transaction_date = dt.full_date
    LEFT JOIN dim_pan dp
        ON stg.client_id = dp.client_id
)

SELECT * FROM joined

'@
$factWallet = @'
{{
    config(
        materialized='incremental',
        unique_key='wallet_transaction_key',
        incremental_strategy='merge',
        tags=['facts'],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS idx_{{ this.name }}_txn_date ON {{ this }} (transaction_date)"
        ]
    )
}}

WITH stg AS (
    SELECT * FROM {{ ref('stg_wallet_transactions') }}
    {% if is_incremental() %}
    WHERE transaction_date > (SELECT MAX(transaction_date) FROM {{ this }})
    {% endif %}
),

dim_atm AS (
    SELECT atm_key, atm_id, geography_key FROM {{ ref('dim_atm') }}
),

dim_ch AS (
    SELECT channel_key, channel_code FROM {{ ref('dim_channel') }}
),

dim_dt AS (
    SELECT date_key, full_date FROM {{ ref('dim_date') }}
),

joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['stg.transaction_id']) }}
                                                AS wallet_transaction_key,
        dt.date_key,
        da.atm_key,
        da.geography_key,
        dch.channel_key,
        stg.transaction_id,
        stg.mobile_number_masked,
        stg.amount_mad,
        stg.transaction_date,
        stg.transaction_hour,
        stg.transaction_type,
        stg.transaction_status,
        stg.is_reversal,
        stg.is_cash_out,
        stg.is_successful,
        stg.currency,
        stg._silver_loaded_at,
        CURRENT_TIMESTAMP                       AS _loaded_at,
        {{ audit_columns() }}
    FROM stg
    LEFT JOIN dim_atm da
        ON stg.atm_id = da.atm_id
    LEFT JOIN dim_ch dch
        ON stg.channel = dch.channel_code
    LEFT JOIN dim_dt dt
        ON stg.transaction_date = dt.full_date
)

SELECT * FROM joined

'@
$factsSchema = @'
version: 2

models:
  - name: fact_atm_transactions
    description: "ATM card transaction fact table — incremental merge on transaction_date"
    columns:
      - name: atm_transaction_key
        description: "Surrogate key (MD5 of refnum)"
        tests: [not_null, unique]
      - name: refnum
        description: "Natural key — proves the grain independently of the surrogate key"
        tests: [not_null, unique]
      - name: atm_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_atm')
              field: atm_key
      - name: customer_key
        tests:
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: geography_key
        description: "Wired via dim_atm's region -> dim_geography join (Phase 1 fix — previously dim_geography was built but never joined into any fact)"
        tests:
          - relationships:
              to: ref('dim_geography')
              field: geography_key
      - name: pan_key
        description: "Wired via pan_customer_map on client_id (Phase 1 fix). Legitimate here because stg_atm_transactions and pan_customer_map share the same Moroccan client_id universe — deliberately NOT done on fact_card_transactions, see that model's note below."
        tests:
          - relationships:
              to: ref('pan_customer_map')
              field: pan_key
      - name: channel_key
        tests:
          - relationships:
              to: ref('dim_channel')
              field: channel_key
      - name: error_type_key
        tests:
          - relationships:
              to: ref('dim_error_type')
              field: error_type_key
      - name: date_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
      - name: amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_successful
        tests: [not_null]
      - name: is_reversal
        tests: [not_null]
      - name: is_deposit
        tests: [not_null]
      - name: is_out_of_cash
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]

  - name: fact_card_transactions
    description: >
      Kaggle card transaction fact table with fraud labels — incremental
      merge on transaction_date. NOTE: this table intentionally does NOT
      carry a pan_key. It is built from stg_transactions (the Kaggle
      dataset), whose client_id/card_id values belong to a different
      synthetic universe than pan_customer_map (which is built from the
      Moroccan cards.csv source). Joining them would create a fabricated
      relationship — coincidental ID collisions presented as a real FK.
      The genuinely Moroccan card-transaction source (stg_card_transactions,
      with real PANs) is currently orphaned from any fact table — see
      repo notes for that separate, larger gap.
    columns:
      - name: card_transaction_key
        description: "Surrogate key (MD5 of transaction_id)"
        tests: [not_null, unique]
      - name: transaction_id
        description: "Natural key — proves the grain independently of the surrogate key"
        tests: [not_null, unique]
      - name: customer_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: card_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_card')
              field: card_key
      - name: merchant_key
        tests:
          - relationships:
              to: ref('dim_merchant')
              field: merchant_key
      - name: merchant_category_key
        tests:
          - relationships:
              to: ref('dim_merchant_category')
              field: merchant_category_key
      - name: channel_key
        tests:
          - relationships:
              to: ref('dim_channel')
              field: channel_key
      - name: error_type_key
        tests:
          - relationships:
              to: ref('dim_error_type')
              field: error_type_key
      - name: date_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
      - name: amount_mad
        tests:
          - not_null
      - name: amount_abs
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_fraud
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
      - name: transaction_date
        tests: [not_null]

  - name: fact_wallet_transactions
    description: "Mobile wallet transaction fact table — incremental merge on transaction_date"
    columns:
      - name: wallet_transaction_key
        description: "Surrogate key (MD5 of transaction_id)"
        tests: [not_null, unique]
      - name: transaction_id
        description: "Natural key — proves the grain independently of the surrogate key"
        tests: [not_null, unique]
      - name: atm_key
        tests:
          - relationships:
              to: ref('dim_atm')
              field: atm_key
      - name: geography_key
        description: "Wired via dim_atm's region -> dim_geography join (Phase 1 fix)"
        tests:
          - relationships:
              to: ref('dim_geography')
              field: geography_key
      - name: channel_key
        tests:
          - relationships:
              to: ref('dim_channel')
              field: channel_key
      - name: date_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
      - name: amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_successful
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]

  - name: fact_out_of_cash_events
    description: "ATM out-of-cash failure event fact table — incremental merge on transaction_date"
    columns:
      - name: out_of_cash_key
        description: "Surrogate key (MD5 of refnum)"
        tests: [not_null, unique]
      - name: refnum
        description: "Natural key — proves the grain independently of the surrogate key"
        tests: [not_null, unique]
      - name: atm_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_atm')
              field: atm_key
      - name: date_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key
      - name: attempted_amount_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: is_confirmed_ooc
        tests: [not_null]
      - name: transaction_date
        tests: [not_null]

'@
$dimsSchema = @'
version: 2

models:

  - name: dim_customer
    description: "Customer dimension — 2,000 Moroccan banking customers with credit and risk attributes"
    columns:
      - name: customer_key
        description: "Surrogate key (MD5 of client_id)"
        tests: [not_null, unique]
      - name: client_id
        tests: [not_null, unique]
      - name: credit_score
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 300
              max_value: 850
      - name: debt_risk_level
        tests:
          - accepted_values:
              values: ['LOW_RISK', 'MEDIUM_RISK', 'HIGH_RISK']
      - name: credit_usage_tier
        tests:
          - accepted_values:
              values: ['LIGHT_USER', 'MODERATE_USER', 'HEAVY_USER']

  - name: dim_card
    description: "Card dimension — 6,146 bank cards with dark web risk flags"
    columns:
      - name: card_key
        tests: [not_null, unique]
      - name: card_id
        tests: [not_null, unique]
      - name: client_id
        tests: [not_null]
      - name: customer_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key
      - name: dark_web_risk
        tests:
          - accepted_values:
              values: ['LOW', 'HIGH']
      - name: card_risk_level
        tests:
          - accepted_values:
              values: ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']

  - name: dim_atm
    description: "ATM dimension — 159 Moroccan ATMs with region, capacity, and maturity attributes"
    columns:
      - name: atm_key
        tests: [not_null, unique]
      - name: atm_id
        tests: [not_null, unique]
      - name: geography_key
        description: >
          Phase 1 fix — previously dim_geography existed but was never
          joined into dim_atm or any fact table. Joined on region only
          (not region+country) since atm_master.csv has no country column
          of its own and country literals differ across the codebase
          ('Morocco' here vs 'Maroc' in config.py) — see dim_atm.sql for
          the full explanation. A NULL here means a region in dim_atm
          didn't find a match in dim_geography and should be investigated,
          not silently ignored.
        tests:
          - relationships:
              to: ref('dim_geography')
              field: geography_key
      - name: cash_limit_mad
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: capacity_tier
        tests:
          - accepted_values:
              values: ['LOW_CAPACITY', 'MEDIUM_CAPACITY', 'HIGH_CAPACITY']
      - name: atm_maturity
        tests:
          - accepted_values:
              values: ['NEW', 'ESTABLISHED', 'MATURE']

  - name: dim_date
    description: "Date dimension — daily spine from 2023-01-01 to 2027-12-31 with Moroccan holidays"
    columns:
      - name: date_key
        description: "Integer date key YYYYMMDD"
        tests: [not_null, unique]
      - name: full_date
        tests: [not_null, unique]
      - name: year
        tests: [not_null]
      - name: month
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 12

  - name: dim_geography
    description: "Geography dimension — Moroccan regions from ATM and merchant locations. Phase 1 fix: now actually joined into dim_atm, fact_atm_transactions, and fact_wallet_transactions — previously built but orphaned."
    columns:
      - name: geography_key
        tests: [not_null, unique]
      - name: region
        tests: [not_null]
      - name: country
        tests: [not_null]
      - name: macro_region
        tests:
          - accepted_values:
              values: ['NORTH', 'SOUTH', 'EAST', 'OTHER']
      - name: population_tier
        tests:
          - accepted_values:
              values: ['TIER_1', 'TIER_2', 'TIER_3']

  - name: dim_merchant
    description: "Merchant dimension — derived from Kaggle transaction source merchants"
    columns:
      - name: merchant_key
        tests: [not_null, unique]
      - name: merchant_id
        tests: [not_null, unique]
      - name: mcc_code
        tests: [not_null]

  - name: dim_merchant_category
    description: "MCC dimension — merchant category codes with risk and group classification"
    columns:
      - name: merchant_category_key
        tests: [not_null, unique]
      - name: mcc_code
        tests: [not_null, unique]
      - name: category_group
        tests: [not_null]

  - name: pan_customer_map
    description: "PAN-to-customer bridge table — links tokenized card PANs to client_id and surrogate keys. Phase 1 fix: now actually joined into fact_atm_transactions — previously built but referenced by zero downstream models."
    columns:
      - name: pan_key
        tests: [not_null, unique]
      - name: pan_masked
        tests: [not_null]
      - name: client_id
        tests: [not_null]
      - name: customer_key
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_key

'@

Write-FileForce -Path "macros\audit_columns.sql" -Content $auditMacro
Write-FileForce -Path "models\marts\dimensions\dim_atm.sql" -Content $dimAtm
Write-FileForce -Path "models\marts\facts\fact_atm_transactions.sql" -Content $factAtm
Write-FileForce -Path "models\marts\facts\fact_wallet_transactions.sql" -Content $factWallet
Write-FileForce -Path "models\marts\facts\schema.yml" -Content $factsSchema
Write-FileForce -Path "models\marts\dimensions\schema.yml" -Content $dimsSchema

Write-Log ""
Write-Log "Phase 1 dbt changes written."
Write-Log "IMPORTANT: dim_atm changed (added geography_key), so downstream"
Write-Log "incremental facts may need a full-refresh to pick up the new column:"
Write-Log ""
Write-Log "    dbt run --full-refresh"
Write-Log "    dbt test"
Write-Log ""
Write-Log "Expect the total test count to increase from 185 -- new relationships"
Write-Log "and uniqueness tests were added. Watch specifically for any"
Write-Log "relationships test failures on geography_key or pan_key, which would"
Write-Log "mean the region/client_id join logic doesn't match your actual data"
Write-Log "as cleanly as assumed -- report that back if it happens, don't just"
Write-Log "delete the test."
