{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Dimension: Users

    Purpose:
    - Provide a complete user dimension for analytics
    - Include calculated lifetime metrics
    - Enable user segmentation analysis

    Grain: One row per user
    Update frequency: Daily
*/

WITH users AS (
    SELECT * FROM {{ ref('stg_users') }}
),

-- Calculate user activity metrics from sessions
user_activity AS (
    SELECT
        user_id,
        COUNT(DISTINCT session_id) AS total_sessions,
        COUNT(DISTINCT session_date) AS active_days,
        MIN(session_date) AS first_activity_date,
        MAX(session_date) AS last_activity_date,
        SUM(total_events) AS total_events,
        SUM(page_views) AS total_page_views,
        SUM(product_views) AS total_product_views,
        SUM(purchases) AS total_purchases,
        SUM(session_revenue) AS total_revenue,
        AVG(session_duration_seconds) AS avg_session_duration,
        AVG(engagement_score) AS avg_engagement_score
    FROM {{ ref('int_sessions') }}
    GROUP BY user_id
),

-- Calculate RFM scores
rfm_scores AS (
    SELECT
        user_id,
        -- Recency: days since last activity
        DATEDIFF('day', last_activity_date, CURRENT_DATE) AS recency_days,
        -- Frequency: total sessions
        total_sessions AS frequency,
        -- Monetary: total revenue
        COALESCE(total_revenue, 0) AS monetary
    FROM user_activity
),

rfm_tiers AS (
    SELECT
        user_id,
        recency_days,
        frequency,
        monetary,
        -- RFM tiers (1-5, 5 being best)
        CASE
            WHEN recency_days <= 7 THEN 5
            WHEN recency_days <= 14 THEN 4
            WHEN recency_days <= 30 THEN 3
            WHEN recency_days <= 60 THEN 2
            ELSE 1
        END AS recency_score,
        NTILE(5) OVER (ORDER BY frequency) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary) AS monetary_score
    FROM rfm_scores
),

final AS (
    SELECT
        -- Primary key
        u.user_id,

        -- User attributes
        u.user_segment,
        u.preferred_device,
        u.acquisition_source,
        u.country_code,
        u.city,

        -- Account details
        u.account_created_at,
        u.account_created_date,
        u.account_age_days,
        u.tenure_tier,
        u.is_email_subscribed,

        -- Activity metrics
        COALESCE(ua.total_sessions, 0) AS lifetime_sessions,
        COALESCE(ua.active_days, 0) AS lifetime_active_days,
        ua.first_activity_date,
        ua.last_activity_date,
        COALESCE(ua.total_events, 0) AS lifetime_events,
        COALESCE(ua.total_page_views, 0) AS lifetime_page_views,
        COALESCE(ua.total_product_views, 0) AS lifetime_product_views,
        COALESCE(ua.total_purchases, 0) AS lifetime_purchases,
        COALESCE(ua.total_revenue, 0) AS lifetime_revenue,
        COALESCE(ua.avg_session_duration, 0) AS avg_session_duration_seconds,
        COALESCE(ua.avg_engagement_score, 0) AS avg_engagement_score,

        -- RFM analysis
        COALESCE(r.recency_days, 999) AS recency_days,
        COALESCE(r.recency_score, 1) AS recency_score,
        COALESCE(r.frequency_score, 1) AS frequency_score,
        COALESCE(r.monetary_score, 1) AS monetary_score,
        COALESCE(r.recency_score, 1) + COALESCE(r.frequency_score, 1) + COALESCE(r.monetary_score, 1) AS rfm_total_score,

        -- Customer value tier
        CASE
            WHEN COALESCE(ua.total_revenue, 0) >= 1000 THEN 'VIP'
            WHEN COALESCE(ua.total_revenue, 0) >= 500 THEN 'High Value'
            WHEN COALESCE(ua.total_revenue, 0) >= 100 THEN 'Medium Value'
            WHEN COALESCE(ua.total_revenue, 0) > 0 THEN 'Low Value'
            ELSE 'No Purchase'
        END AS customer_value_tier,

        -- Activity status
        CASE
            WHEN ua.last_activity_date IS NULL THEN 'Never Active'
            WHEN DATEDIFF('day', ua.last_activity_date, CURRENT_DATE) <= 7 THEN 'Active'
            WHEN DATEDIFF('day', ua.last_activity_date, CURRENT_DATE) <= 30 THEN 'Recent'
            WHEN DATEDIFF('day', ua.last_activity_date, CURRENT_DATE) <= 90 THEN 'Lapsing'
            ELSE 'Churned'
        END AS activity_status,

        -- Conversion status
        CASE
            WHEN COALESCE(ua.total_purchases, 0) > 1 THEN 'Repeat Buyer'
            WHEN COALESCE(ua.total_purchases, 0) = 1 THEN 'One-Time Buyer'
            ELSE 'Non-Buyer'
        END AS buyer_status,

        -- Metadata
        CURRENT_TIMESTAMP AS _updated_at

    FROM users u
    LEFT JOIN user_activity ua ON u.user_id = ua.user_id
    LEFT JOIN rfm_tiers r ON u.user_id = r.user_id
)

SELECT * FROM final
