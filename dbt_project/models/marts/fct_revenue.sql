{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'revenue']
    )
}}

/*
    Fact: Revenue Analytics

    Purpose:
    - Track revenue metrics across dimensions
    - Enable revenue forecasting and analysis
    - Support financial reporting

    Grain: One row per date x category
    Update frequency: Daily
*/

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    WHERE event_type = 'purchase' AND revenue_amount > 0
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

-- Daily revenue by category
daily_category_revenue AS (
    SELECT
        e.event_date,
        COALESCE(e.product_category, 'Unknown') AS category,

        -- Transaction counts
        COUNT(DISTINCT e.event_id) AS transaction_count,
        COUNT(DISTINCT e.user_id) AS purchasing_users,
        COUNT(DISTINCT e.session_id) AS purchasing_sessions,

        -- Revenue metrics
        SUM(e.revenue_amount) AS gross_revenue,
        AVG(e.revenue_amount) AS avg_transaction_value,
        MIN(e.revenue_amount) AS min_transaction_value,
        MAX(e.revenue_amount) AS max_transaction_value,

        -- Product metrics
        COUNT(DISTINCT e.product_id) AS unique_products_sold,

        -- Device breakdown
        SUM(CASE WHEN e.device_type = 'mobile' THEN e.revenue_amount ELSE 0 END) AS mobile_revenue,
        SUM(CASE WHEN e.device_type = 'desktop' THEN e.revenue_amount ELSE 0 END) AS desktop_revenue,
        SUM(CASE WHEN e.device_type = 'tablet' THEN e.revenue_amount ELSE 0 END) AS tablet_revenue,

        -- Traffic source breakdown
        SUM(CASE WHEN e.traffic_source = 'organic' THEN e.revenue_amount ELSE 0 END) AS organic_revenue,
        SUM(CASE WHEN e.traffic_source = 'paid_search' THEN e.revenue_amount ELSE 0 END) AS paid_revenue,
        SUM(CASE WHEN e.traffic_source = 'social' THEN e.revenue_amount ELSE 0 END) AS social_revenue,
        SUM(CASE WHEN e.traffic_source = 'email' THEN e.revenue_amount ELSE 0 END) AS email_revenue,
        SUM(CASE WHEN e.traffic_source = 'direct' THEN e.revenue_amount ELSE 0 END) AS direct_revenue

    FROM events e
    GROUP BY e.event_date, COALESCE(e.product_category, 'Unknown')
),

-- Add category metadata
with_category_info AS (
    SELECT
        dcr.*,

        -- Category average price (for context)
        AVG(p.price) AS category_avg_price,

        -- Revenue share
        SUM(dcr.gross_revenue) OVER (PARTITION BY dcr.event_date) AS daily_total_revenue,
        dcr.gross_revenue / NULLIF(SUM(dcr.gross_revenue) OVER (PARTITION BY dcr.event_date), 0) AS category_revenue_share

    FROM daily_category_revenue dcr
    LEFT JOIN products p ON dcr.category = p.category
    GROUP BY
        dcr.event_date,
        dcr.category,
        dcr.transaction_count,
        dcr.purchasing_users,
        dcr.purchasing_sessions,
        dcr.gross_revenue,
        dcr.avg_transaction_value,
        dcr.min_transaction_value,
        dcr.max_transaction_value,
        dcr.unique_products_sold,
        dcr.mobile_revenue,
        dcr.desktop_revenue,
        dcr.tablet_revenue,
        dcr.organic_revenue,
        dcr.paid_revenue,
        dcr.social_revenue,
        dcr.email_revenue,
        dcr.direct_revenue
),

with_rolling AS (
    SELECT
        *,

        -- 7-day rolling metrics
        SUM(gross_revenue) OVER (
            PARTITION BY category
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS revenue_7d_rolling,
        AVG(gross_revenue) OVER (
            PARTITION BY category
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS revenue_7d_avg,

        -- 30-day rolling metrics
        SUM(gross_revenue) OVER (
            PARTITION BY category
            ORDER BY event_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS revenue_30d_rolling,

        -- Period comparisons
        LAG(gross_revenue, 1) OVER (PARTITION BY category ORDER BY event_date) AS prev_day_revenue,
        LAG(gross_revenue, 7) OVER (PARTITION BY category ORDER BY event_date) AS prev_week_revenue,

        -- Cumulative metrics
        SUM(gross_revenue) OVER (
            PARTITION BY category
            ORDER BY event_date
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_revenue

    FROM with_category_info
),

final AS (
    SELECT
        *,

        -- Growth rates
        CASE
            WHEN prev_day_revenue > 0
            THEN (gross_revenue - prev_day_revenue) / prev_day_revenue
            ELSE 0
        END AS revenue_dod_growth,
        CASE
            WHEN prev_week_revenue > 0
            THEN (gross_revenue - prev_week_revenue) / prev_week_revenue
            ELSE 0
        END AS revenue_wow_growth,

        -- Metadata
        CURRENT_TIMESTAMP AS _updated_at

    FROM with_rolling
)

SELECT * FROM final
ORDER BY event_date, category
