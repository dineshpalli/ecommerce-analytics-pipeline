{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Dimension: Date

    Purpose:
    - Provide a complete date dimension for analytics
    - Enable time-based analysis and filtering
    - Support calendar-based reporting

    Grain: One row per date
    Range: Covers data range plus buffer
*/

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2023-01-01' as date)",
        end_date="cast(current_date + interval '30 days' as date)"
    ) }}
),

enriched AS (
    SELECT
        -- Primary key
        CAST(date_day AS DATE) AS date_key,

        -- Date components
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(DOY FROM date_day) AS day_of_year,
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(DOW FROM date_day) AS day_of_week,

        -- Date names
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_short,
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_short,

        -- Formatted dates
        TO_CHAR(date_day, 'YYYY-MM-DD') AS date_iso,
        TO_CHAR(date_day, 'YYYYMMDD') AS date_int,
        TO_CHAR(date_day, 'MM/DD/YYYY') AS date_us,
        TO_CHAR(date_day, 'DD.MM.YYYY') AS date_eu,

        -- Fiscal periods (assuming calendar year = fiscal year)
        EXTRACT(YEAR FROM date_day) AS fiscal_year,
        EXTRACT(QUARTER FROM date_day) AS fiscal_quarter,

        -- Week boundaries
        DATE_TRUNC('week', date_day) AS week_start,
        DATE_TRUNC('week', date_day) + INTERVAL '6 days' AS week_end,

        -- Month boundaries
        DATE_TRUNC('month', date_day) AS month_start,
        (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end,

        -- Quarter boundaries
        DATE_TRUNC('quarter', date_day) AS quarter_start,
        (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months' - INTERVAL '1 day')::DATE AS quarter_end,

        -- Year boundaries
        DATE_TRUNC('year', date_day) AS year_start,
        (DATE_TRUNC('year', date_day) + INTERVAL '1 year' - INTERVAL '1 day')::DATE AS year_end,

        -- Boolean flags
        CASE WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN EXTRACT(DOW FROM date_day) NOT IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekday,
        CASE WHEN date_day = DATE_TRUNC('month', date_day) THEN TRUE ELSE FALSE END AS is_month_start,
        CASE WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN TRUE ELSE FALSE END AS is_month_end,
        CASE WHEN date_day = DATE_TRUNC('year', date_day) THEN TRUE ELSE FALSE END AS is_year_start,
        CASE WHEN date_day = (DATE_TRUNC('year', date_day) + INTERVAL '1 year' - INTERVAL '1 day')::DATE THEN TRUE ELSE FALSE END AS is_year_end,

        -- Relative date flags
        CASE WHEN date_day = CURRENT_DATE THEN TRUE ELSE FALSE END AS is_today,
        CASE WHEN date_day = CURRENT_DATE - INTERVAL '1 day' THEN TRUE ELSE FALSE END AS is_yesterday,
        CASE WHEN date_day BETWEEN CURRENT_DATE - INTERVAL '7 days' AND CURRENT_DATE - INTERVAL '1 day' THEN TRUE ELSE FALSE END AS is_last_7_days,
        CASE WHEN date_day BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE - INTERVAL '1 day' THEN TRUE ELSE FALSE END AS is_last_30_days,
        CASE WHEN date_day BETWEEN CURRENT_DATE - INTERVAL '90 days' AND CURRENT_DATE - INTERVAL '1 day' THEN TRUE ELSE FALSE END AS is_last_90_days,

        -- Prior period comparisons
        date_day - INTERVAL '7 days' AS same_day_last_week,
        date_day - INTERVAL '1 month' AS same_day_last_month,
        date_day - INTERVAL '1 year' AS same_day_last_year

    FROM date_spine
)

SELECT * FROM enriched
