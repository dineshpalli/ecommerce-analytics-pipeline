{{
    config(
        materialized='view',
        tags=['intermediate', 'sessions']
    )
}}

/*
    Intermediate model: Session-level aggregations

    Purpose:
    - Aggregate events to session level
    - Calculate session metrics (duration, engagement)
    - Identify conversion within sessions

    Grain: One row per session
*/

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

session_metrics AS (
    SELECT
        -- Session identifiers
        session_id,
        user_id,

        -- Session timing
        MIN(event_timestamp) AS session_start,
        MAX(event_timestamp) AS session_end,
        DATEDIFF('second', MIN(event_timestamp), MAX(event_timestamp)) AS session_duration_seconds,

        -- Session date (for partitioning/filtering)
        MIN(event_date) AS session_date,

        -- Device and location (first touch)
        MIN(device_type) AS device_type,
        MIN(country_code) AS country_code,
        MIN(traffic_source) AS traffic_source,

        -- Event counts
        COUNT(*) AS total_events,
        COUNT(DISTINCT event_type) AS unique_event_types,

        -- Engagement metrics by event type
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
        SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS searches,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
        SUM(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_events,
        SUM(CASE WHEN event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS checkout_starts,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,

        -- Product engagement
        COUNT(DISTINCT product_id) AS unique_products_viewed,
        COUNT(DISTINCT product_category) AS unique_categories_viewed,

        -- Revenue
        SUM(revenue_amount) AS session_revenue,
        MAX(revenue_amount) AS max_order_value

    FROM events
    GROUP BY session_id, user_id
),

enriched AS (
    SELECT
        *,

        -- Duration categories
        CASE
            WHEN session_duration_seconds < 30 THEN 'Bounce'
            WHEN session_duration_seconds < 120 THEN 'Quick'
            WHEN session_duration_seconds < 600 THEN 'Engaged'
            WHEN session_duration_seconds < 1800 THEN 'Deep'
            ELSE 'Extended'
        END AS duration_category,

        -- Conversion flags
        CASE WHEN purchases > 0 THEN TRUE ELSE FALSE END AS is_converted,
        CASE WHEN add_to_cart_events > 0 THEN TRUE ELSE FALSE END AS has_cart_activity,
        CASE WHEN checkout_starts > 0 THEN TRUE ELSE FALSE END AS started_checkout,

        -- Engagement score (simple weighted metric)
        (
            page_views * 1 +
            product_views * 2 +
            searches * 1.5 +
            add_to_cart_events * 5 +
            checkout_starts * 10 +
            purchases * 20
        ) AS engagement_score,

        -- Session quality tier
        CASE
            WHEN purchases > 0 THEN 'High Value'
            WHEN checkout_starts > 0 THEN 'High Intent'
            WHEN add_to_cart_events > 0 THEN 'Engaged'
            WHEN product_views >= 3 THEN 'Browsing'
            ELSE 'Low Engagement'
        END AS session_quality

    FROM session_metrics
    -- Filter out suspicious sessions
    WHERE session_duration_seconds <= {{ var('max_session_hours') }} * 3600
)

SELECT * FROM enriched
