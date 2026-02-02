{{
    config(
        materialized='view',
        tags=['intermediate', 'funnel']
    )
}}

/*
    Intermediate model: User funnel progression

    Purpose:
    - Track user progression through the conversion funnel
    - Calculate time between funnel steps
    - Enable cohort and funnel analysis

    Grain: One row per user per funnel stage reached
*/

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

-- Define funnel stages
funnel_stages AS (
    SELECT
        user_id,
        session_id,
        event_date,

        -- First occurrence of each stage
        MIN(CASE WHEN event_type = 'page_view' THEN event_timestamp END) AS first_page_view,
        MIN(CASE WHEN event_type = 'product_view' THEN event_timestamp END) AS first_product_view,
        MIN(CASE WHEN event_type = 'add_to_cart' THEN event_timestamp END) AS first_add_to_cart,
        MIN(CASE WHEN event_type = 'begin_checkout' THEN event_timestamp END) AS first_checkout,
        MIN(CASE WHEN event_type = 'purchase' THEN event_timestamp END) AS first_purchase,

        -- Event counts per stage
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count,
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_view_count,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_count,
        SUM(CASE WHEN event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS checkout_count,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,

        -- Revenue
        SUM(revenue_amount) AS total_revenue

    FROM events
    GROUP BY user_id, session_id, event_date
),

with_progression AS (
    SELECT
        *,

        -- Reached stage flags
        CASE WHEN first_page_view IS NOT NULL THEN TRUE ELSE FALSE END AS reached_site,
        CASE WHEN first_product_view IS NOT NULL THEN TRUE ELSE FALSE END AS reached_product,
        CASE WHEN first_add_to_cart IS NOT NULL THEN TRUE ELSE FALSE END AS reached_cart,
        CASE WHEN first_checkout IS NOT NULL THEN TRUE ELSE FALSE END AS reached_checkout,
        CASE WHEN first_purchase IS NOT NULL THEN TRUE ELSE FALSE END AS reached_purchase,

        -- Deepest funnel stage reached
        CASE
            WHEN first_purchase IS NOT NULL THEN 'purchase'
            WHEN first_checkout IS NOT NULL THEN 'checkout'
            WHEN first_add_to_cart IS NOT NULL THEN 'cart'
            WHEN first_product_view IS NOT NULL THEN 'product_view'
            WHEN first_page_view IS NOT NULL THEN 'page_view'
            ELSE 'none'
        END AS deepest_stage,

        -- Time between stages (in seconds)
        DATEDIFF('second', first_page_view, first_product_view) AS time_to_product_view,
        DATEDIFF('second', first_product_view, first_add_to_cart) AS time_to_cart,
        DATEDIFF('second', first_add_to_cart, first_checkout) AS time_to_checkout,
        DATEDIFF('second', first_checkout, first_purchase) AS time_to_purchase,
        DATEDIFF('second', first_page_view, first_purchase) AS total_time_to_purchase

    FROM funnel_stages
),

final AS (
    SELECT
        *,

        -- Funnel stage number (for aggregations)
        CASE deepest_stage
            WHEN 'none' THEN 0
            WHEN 'page_view' THEN 1
            WHEN 'product_view' THEN 2
            WHEN 'cart' THEN 3
            WHEN 'checkout' THEN 4
            WHEN 'purchase' THEN 5
        END AS funnel_depth,

        -- Drop-off point
        CASE
            WHEN reached_purchase THEN 'converted'
            WHEN reached_checkout AND NOT reached_purchase THEN 'checkout_abandoned'
            WHEN reached_cart AND NOT reached_checkout THEN 'cart_abandoned'
            WHEN reached_product AND NOT reached_cart THEN 'product_exit'
            WHEN reached_site AND NOT reached_product THEN 'landing_bounce'
            ELSE 'no_engagement'
        END AS drop_off_stage

    FROM with_progression
)

SELECT * FROM final
