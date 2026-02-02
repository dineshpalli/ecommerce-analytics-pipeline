{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'funnel']
    )
}}

/*
    Fact: Conversion Funnel Metrics

    Purpose:
    - Track conversion funnel performance
    - Calculate stage-by-stage drop-off rates
    - Enable funnel optimization analysis

    Grain: One row per date
    Update frequency: Daily
*/

WITH user_journey AS (
    SELECT * FROM {{ ref('int_user_journey') }}
),

daily_funnel AS (
    SELECT
        event_date,

        -- Stage counts
        COUNT(DISTINCT CASE WHEN reached_site THEN user_id END) AS users_at_site,
        COUNT(DISTINCT CASE WHEN reached_product THEN user_id END) AS users_at_product,
        COUNT(DISTINCT CASE WHEN reached_cart THEN user_id END) AS users_at_cart,
        COUNT(DISTINCT CASE WHEN reached_checkout THEN user_id END) AS users_at_checkout,
        COUNT(DISTINCT CASE WHEN reached_purchase THEN user_id END) AS users_at_purchase,

        -- Session counts per stage
        COUNT(DISTINCT CASE WHEN reached_site THEN session_id END) AS sessions_at_site,
        COUNT(DISTINCT CASE WHEN reached_product THEN session_id END) AS sessions_at_product,
        COUNT(DISTINCT CASE WHEN reached_cart THEN session_id END) AS sessions_at_cart,
        COUNT(DISTINCT CASE WHEN reached_checkout THEN session_id END) AS sessions_at_checkout,
        COUNT(DISTINCT CASE WHEN reached_purchase THEN session_id END) AS sessions_at_purchase,

        -- Drop-off counts
        COUNT(DISTINCT CASE WHEN drop_off_stage = 'landing_bounce' THEN user_id END) AS bounced_users,
        COUNT(DISTINCT CASE WHEN drop_off_stage = 'product_exit' THEN user_id END) AS product_exit_users,
        COUNT(DISTINCT CASE WHEN drop_off_stage = 'cart_abandoned' THEN user_id END) AS cart_abandoned_users,
        COUNT(DISTINCT CASE WHEN drop_off_stage = 'checkout_abandoned' THEN user_id END) AS checkout_abandoned_users,
        COUNT(DISTINCT CASE WHEN drop_off_stage = 'converted' THEN user_id END) AS converted_users,

        -- Revenue
        SUM(total_revenue) AS total_revenue,

        -- Average time metrics
        AVG(time_to_product_view) AS avg_time_to_product_view,
        AVG(time_to_cart) AS avg_time_to_cart,
        AVG(time_to_checkout) AS avg_time_to_checkout,
        AVG(time_to_purchase) AS avg_time_to_purchase,
        AVG(total_time_to_purchase) AS avg_total_time_to_purchase

    FROM user_journey
    GROUP BY event_date
),

with_rates AS (
    SELECT
        *,

        -- Stage conversion rates (from previous stage)
        CASE WHEN users_at_site > 0 THEN CAST(users_at_product AS FLOAT) / users_at_site ELSE 0 END AS site_to_product_rate,
        CASE WHEN users_at_product > 0 THEN CAST(users_at_cart AS FLOAT) / users_at_product ELSE 0 END AS product_to_cart_rate,
        CASE WHEN users_at_cart > 0 THEN CAST(users_at_checkout AS FLOAT) / users_at_cart ELSE 0 END AS cart_to_checkout_rate,
        CASE WHEN users_at_checkout > 0 THEN CAST(users_at_purchase AS FLOAT) / users_at_checkout ELSE 0 END AS checkout_to_purchase_rate,

        -- Overall conversion rate
        CASE WHEN users_at_site > 0 THEN CAST(users_at_purchase AS FLOAT) / users_at_site ELSE 0 END AS overall_conversion_rate,

        -- Drop-off rates
        CASE WHEN users_at_site > 0 THEN CAST(bounced_users AS FLOAT) / users_at_site ELSE 0 END AS bounce_rate,
        CASE WHEN users_at_cart > 0 THEN CAST(cart_abandoned_users AS FLOAT) / users_at_cart ELSE 0 END AS cart_abandonment_rate,
        CASE WHEN users_at_checkout > 0 THEN CAST(checkout_abandoned_users AS FLOAT) / users_at_checkout ELSE 0 END AS checkout_abandonment_rate,

        -- Average order value
        CASE WHEN users_at_purchase > 0 THEN total_revenue / users_at_purchase ELSE 0 END AS avg_order_value,

        -- Revenue per site visitor
        CASE WHEN users_at_site > 0 THEN total_revenue / users_at_site ELSE 0 END AS revenue_per_visitor

    FROM daily_funnel
),

with_comparisons AS (
    SELECT
        *,

        -- 7-day averages
        AVG(overall_conversion_rate) OVER (
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS conversion_rate_7d_avg,
        AVG(cart_abandonment_rate) OVER (
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS cart_abandonment_7d_avg,

        -- Week-over-week comparisons
        LAG(overall_conversion_rate, 7) OVER (ORDER BY event_date) AS prev_week_conversion_rate,
        LAG(cart_abandonment_rate, 7) OVER (ORDER BY event_date) AS prev_week_cart_abandonment,

        -- Metadata
        CURRENT_TIMESTAMP AS _updated_at

    FROM with_rates
)

SELECT * FROM with_comparisons
ORDER BY event_date
