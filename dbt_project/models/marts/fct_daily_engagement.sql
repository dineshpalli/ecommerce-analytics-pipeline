{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'kpi']
    )
}}

/*
    Fact: Daily Engagement Metrics

    Purpose:
    - Provide daily KPIs for executive dashboards
    - Track user engagement trends
    - Enable period-over-period comparisons

    Grain: One row per date
    Update frequency: Daily
*/

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
),

sessions AS (
    SELECT * FROM {{ ref('int_sessions') }}
),

daily_events AS (
    SELECT
        event_date,

        -- User metrics
        COUNT(DISTINCT user_id) AS daily_active_users,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(*) AS total_events,

        -- Event breakdown
        SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
        SUM(CASE WHEN event_type = 'product_view' THEN 1 ELSE 0 END) AS product_views,
        SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS searches,
        SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS add_to_cart_events,
        SUM(CASE WHEN event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS checkout_starts,
        SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
        SUM(CASE WHEN event_type = 'signup' THEN 1 ELSE 0 END) AS signups,
        SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) AS logins,

        -- Revenue
        SUM(revenue_amount) AS total_revenue,

        -- Product engagement
        COUNT(DISTINCT product_id) AS unique_products_viewed,
        COUNT(DISTINCT product_category) AS unique_categories_viewed,

        -- Device breakdown
        SUM(CASE WHEN device_type = 'mobile' THEN 1 ELSE 0 END) AS mobile_events,
        SUM(CASE WHEN device_type = 'desktop' THEN 1 ELSE 0 END) AS desktop_events,
        SUM(CASE WHEN device_type = 'tablet' THEN 1 ELSE 0 END) AS tablet_events,

        -- Traffic source breakdown
        SUM(CASE WHEN traffic_source = 'organic' THEN 1 ELSE 0 END) AS organic_events,
        SUM(CASE WHEN traffic_source = 'paid_search' THEN 1 ELSE 0 END) AS paid_events,
        SUM(CASE WHEN traffic_source = 'social' THEN 1 ELSE 0 END) AS social_events,
        SUM(CASE WHEN traffic_source = 'email' THEN 1 ELSE 0 END) AS email_events,
        SUM(CASE WHEN traffic_source = 'direct' THEN 1 ELSE 0 END) AS direct_events

    FROM events
    GROUP BY event_date
),

daily_sessions AS (
    SELECT
        session_date,
        COUNT(DISTINCT session_id) AS sessions,
        COUNT(DISTINCT CASE WHEN is_converted THEN session_id END) AS converted_sessions,
        AVG(session_duration_seconds) AS avg_session_duration,
        AVG(total_events) AS avg_events_per_session,
        AVG(engagement_score) AS avg_engagement_score
    FROM sessions
    GROUP BY session_date
),

-- Calculate rolling metrics
with_rolling AS (
    SELECT
        de.*,
        ds.converted_sessions,
        ds.avg_session_duration,
        ds.avg_events_per_session,
        ds.avg_engagement_score,

        -- Conversion rate
        CASE
            WHEN de.total_sessions > 0
            THEN CAST(ds.converted_sessions AS FLOAT) / de.total_sessions
            ELSE 0
        END AS conversion_rate,

        -- Average order value
        CASE
            WHEN de.purchases > 0
            THEN de.total_revenue / de.purchases
            ELSE 0
        END AS avg_order_value,

        -- Events per user
        CASE
            WHEN de.daily_active_users > 0
            THEN CAST(de.total_events AS FLOAT) / de.daily_active_users
            ELSE 0
        END AS events_per_user,

        -- Revenue per user
        CASE
            WHEN de.daily_active_users > 0
            THEN de.total_revenue / de.daily_active_users
            ELSE 0
        END AS revenue_per_user,

        -- 7-day rolling averages
        AVG(de.daily_active_users) OVER (
            ORDER BY de.event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS dau_7d_avg,
        AVG(de.total_revenue) OVER (
            ORDER BY de.event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS revenue_7d_avg,

        -- 7-day totals
        SUM(de.daily_active_users) OVER (
            ORDER BY de.event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS wau_approximation,

        -- Day-over-day changes
        LAG(de.daily_active_users, 1) OVER (ORDER BY de.event_date) AS prev_day_dau,
        LAG(de.total_revenue, 1) OVER (ORDER BY de.event_date) AS prev_day_revenue,
        LAG(de.purchases, 1) OVER (ORDER BY de.event_date) AS prev_day_purchases,

        -- Week-over-week changes
        LAG(de.daily_active_users, 7) OVER (ORDER BY de.event_date) AS prev_week_dau,
        LAG(de.total_revenue, 7) OVER (ORDER BY de.event_date) AS prev_week_revenue

    FROM daily_events de
    LEFT JOIN daily_sessions ds ON de.event_date = ds.session_date
),

final AS (
    SELECT
        *,

        -- Day-over-day change rates
        CASE
            WHEN prev_day_dau > 0
            THEN (daily_active_users - prev_day_dau) / CAST(prev_day_dau AS FLOAT)
            ELSE 0
        END AS dau_dod_change,
        CASE
            WHEN prev_day_revenue > 0
            THEN (total_revenue - prev_day_revenue) / prev_day_revenue
            ELSE 0
        END AS revenue_dod_change,

        -- Week-over-week change rates
        CASE
            WHEN prev_week_dau > 0
            THEN (daily_active_users - prev_week_dau) / CAST(prev_week_dau AS FLOAT)
            ELSE 0
        END AS dau_wow_change,
        CASE
            WHEN prev_week_revenue > 0
            THEN (total_revenue - prev_week_revenue) / prev_week_revenue
            ELSE 0
        END AS revenue_wow_change,

        -- Metadata
        CURRENT_TIMESTAMP AS _updated_at

    FROM with_rolling
)

SELECT * FROM final
ORDER BY event_date
