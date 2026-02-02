{{
    config(
        materialized='view',
        tags=['staging', 'users']
    )
}}

/*
    Staging model for user profiles

    Purpose:
    - Clean and standardize user data
    - Prepare for dimension table
    - Calculate user tenure

    Grain: One row per user
*/

WITH source AS (
    SELECT * FROM {{ source('seeds', 'users') }}
),

cleaned AS (
    SELECT
        -- Primary key
        user_id,

        -- User segmentation
        COALESCE(segment, 'unknown') AS user_segment,

        -- Device preferences
        COALESCE(primary_device, 'unknown') AS preferred_device,

        -- Acquisition
        COALESCE(traffic_source, 'direct') AS acquisition_source,

        -- Geography
        COALESCE(country, 'XX') AS country_code,
        city,

        -- Account details
        created_at AS account_created_at,
        CAST(created_at AS DATE) AS account_created_date,

        -- Calculate tenure
        DATEDIFF('day', created_at, CURRENT_TIMESTAMP) AS account_age_days,
        CASE
            WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP) <= 30 THEN 'New'
            WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP) <= 90 THEN 'Developing'
            WHEN DATEDIFF('day', created_at, CURRENT_TIMESTAMP) <= 365 THEN 'Established'
            ELSE 'Veteran'
        END AS tenure_tier,

        -- Engagement
        COALESCE(is_subscribed, FALSE) AS is_email_subscribed,
        COALESCE(lifetime_value, 0) AS historical_ltv,

        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at

    FROM source
    WHERE user_id IS NOT NULL
)

SELECT * FROM cleaned
