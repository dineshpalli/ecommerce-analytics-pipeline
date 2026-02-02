{{
    config(
        materialized='view',
        tags=['staging', 'events']
    )
}}

/*
    Staging model for raw events

    Purpose:
    - Clean and standardize raw event data
    - Parse JSON properties
    - Add derived fields for downstream use

    Grain: One row per event
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'events') }}
),

cleaned AS (
    SELECT
        -- Primary identifiers
        event_id,
        event_type,
        user_id,
        session_id,

        -- Timestamp fields
        timestamp AS event_timestamp,
        CAST(timestamp AS DATE) AS event_date,
        EXTRACT(HOUR FROM timestamp) AS event_hour,
        EXTRACT(DOW FROM timestamp) AS day_of_week,
        CASE
            WHEN EXTRACT(DOW FROM timestamp) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        -- Device and location
        COALESCE(device, 'unknown') AS device_type,
        COALESCE(country, 'XX') AS country_code,
        COALESCE(traffic_source, 'direct') AS traffic_source,

        -- Product information
        product_id,
        category AS product_category,

        -- Revenue (only for purchase events)
        COALESCE(revenue, 0) AS revenue_amount,
        CASE WHEN event_type = 'purchase' AND revenue > 0 THEN TRUE ELSE FALSE END AS is_revenue_event,

        -- Properties JSON (for advanced analysis)
        properties AS event_properties,

        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at

    FROM source
    WHERE
        -- Basic data quality filters
        event_id IS NOT NULL
        AND user_id IS NOT NULL
        AND timestamp IS NOT NULL
        -- Filter out future timestamps
        AND timestamp <= CURRENT_TIMESTAMP
)

SELECT * FROM cleaned
