{{ config(materialized='table', tags=['dimensions']) }}

WITH silver_pan_map AS (
    SELECT * FROM {{ source('silver', 'pan_customer_map') }}
),

customers AS (
    SELECT
        client_id,
        {{ dbt_utils.generate_surrogate_key(['client_id']) }} AS customer_key
    FROM {{ ref('dim_customer') }}
),

cards AS (
    SELECT
        card_id,
        client_id,
        {{ dbt_utils.generate_surrogate_key(['card_id']) }} AS card_key
    FROM {{ ref('dim_card') }}
),

-- assign one card per customer deterministically
customer_cards AS (
    SELECT
        client_id,
        card_id,
        card_key,
        ROW_NUMBER() OVER (
            PARTITION BY client_id ORDER BY card_id
        ) AS rn
    FROM cards
),

primary_cards AS (
    SELECT client_id, card_id, card_key
    FROM customer_cards
    WHERE rn = 1
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['p.pan_masked']) }} AS pan_key,
        CAST(p.pan_masked AS VARCHAR)                            AS pan_masked,
        CAST(p.pan_num AS BIGINT)                                AS pan_number,
        CAST(p.client_id AS VARCHAR)                             AS client_id,
        c.customer_key,
        pc.card_id,
        pc.card_key,
        p._created_at
    FROM silver_pan_map p
    LEFT JOIN customers c
        ON p.client_id = c.client_id
    LEFT JOIN primary_cards pc
        ON p.client_id = pc.client_id
    WHERE p.pan_masked IS NOT NULL
)

SELECT * FROM enriched