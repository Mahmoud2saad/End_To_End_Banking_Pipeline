{{ config(materialized='table', tags=['dimensions']) }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2010-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}
),

enriched AS (
    SELECT
        CAST(date_day AS DATE)                          AS full_date,
        CAST(STRFTIME(date_day, '%Y%m%d') AS INTEGER)  AS date_key,
        EXTRACT(YEAR FROM date_day)                     AS year,
        EXTRACT(QUARTER FROM date_day)                  AS quarter,
        EXTRACT(MONTH FROM date_day)                    AS month,
        STRFTIME(date_day, '%B')                        AS month_name,
        EXTRACT(WEEK FROM date_day)                     AS week_number,
        EXTRACT(DOW FROM date_day)                      AS day_of_week,
        STRFTIME(date_day, '%A')                        AS day_name,
        EXTRACT(DAY FROM date_day)                      AS day_of_month,
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6)
             THEN TRUE ELSE FALSE END                   AS is_weekend,
        CASE WHEN STRFTIME(date_day, '%m-%d') IN (
            '01-01', '01-11', '03-03', '05-01',
            '07-30', '08-14', '08-20', '11-06', '11-18'
        ) THEN TRUE ELSE FALSE END                      AS is_moroccan_holiday,
        CONCAT(
            CAST(EXTRACT(YEAR FROM date_day) AS VARCHAR),
            '-Q',
            CAST(EXTRACT(QUARTER FROM date_day) AS VARCHAR)
        )                                               AS year_quarter,
        CONCAT(
            CAST(EXTRACT(YEAR FROM date_day) AS VARCHAR),
            '-',
            LPAD(CAST(EXTRACT(MONTH FROM date_day) AS VARCHAR), 2, '0')
        )                                               AS year_month
    FROM date_spine
)

SELECT * FROM enriched