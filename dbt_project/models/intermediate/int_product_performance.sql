{{
    config(
        materialized='view',
        tags=['intermediate', 'products']
    )
}}

/*
    Intermediate model: Product interaction aggregations

    Purpose:
    - Aggregate product-level metrics
    - Calculate conversion rates per product
    - Enable product performance analysis

    Grain: One row per product
*/

WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    WHERE product_id IS NOT NULL
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

product_metrics AS (
    SELECT
        e.product_id,

        -- View metrics
        COUNT(DISTINCT CASE WHEN e.event_type = 'product_view' THEN e.event_id END) AS total_views,
        COUNT(DISTINCT CASE WHEN e.event_type = 'product_view' THEN e.user_id END) AS unique_viewers,
        COUNT(DISTINCT CASE WHEN e.event_type = 'product_view' THEN e.session_id END) AS view_sessions,

        -- Cart metrics
        COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.event_id END) AS cart_adds,
        COUNT(DISTINCT CASE WHEN e.event_type = 'add_to_cart' THEN e.user_id END) AS users_added_to_cart,
        COUNT(DISTINCT CASE WHEN e.event_type = 'remove_from_cart' THEN e.event_id END) AS cart_removes,

        -- Purchase metrics
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.event_id END) AS purchases,
        COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.user_id END) AS purchasing_users,
        SUM(CASE WHEN e.event_type = 'purchase' THEN e.revenue_amount ELSE 0 END) AS total_revenue,

        -- Time range
        MIN(e.event_date) AS first_interaction_date,
        MAX(e.event_date) AS last_interaction_date,
        COUNT(DISTINCT e.event_date) AS active_days

    FROM events e
    GROUP BY e.product_id
),

with_rates AS (
    SELECT
        pm.*,

        -- Conversion rates
        CASE
            WHEN pm.total_views > 0
            THEN CAST(pm.cart_adds AS FLOAT) / pm.total_views
            ELSE 0
        END AS view_to_cart_rate,

        CASE
            WHEN pm.cart_adds > 0
            THEN CAST(pm.purchases AS FLOAT) / pm.cart_adds
            ELSE 0
        END AS cart_to_purchase_rate,

        CASE
            WHEN pm.total_views > 0
            THEN CAST(pm.purchases AS FLOAT) / pm.total_views
            ELSE 0
        END AS overall_conversion_rate,

        -- Average order value
        CASE
            WHEN pm.purchases > 0
            THEN pm.total_revenue / pm.purchases
            ELSE 0
        END AS avg_order_value,

        -- Cart abandonment
        CASE
            WHEN pm.cart_adds > 0
            THEN CAST(pm.cart_adds - pm.purchases AS FLOAT) / pm.cart_adds
            ELSE 0
        END AS cart_abandonment_rate

    FROM product_metrics pm
),

enriched AS (
    SELECT
        wr.*,

        -- Product attributes
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,
        p.price,
        p.price_tier,
        p.rating,
        p.rating_tier,
        p.is_in_stock,

        -- Performance tiers
        CASE
            WHEN wr.total_revenue >= PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY wr.total_revenue) OVER () THEN 'Top Performer'
            WHEN wr.total_revenue >= PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY wr.total_revenue) OVER () THEN 'Above Average'
            WHEN wr.total_revenue > 0 THEN 'Below Average'
            ELSE 'No Sales'
        END AS revenue_tier,

        CASE
            WHEN wr.overall_conversion_rate >= 0.10 THEN 'High Converting'
            WHEN wr.overall_conversion_rate >= 0.05 THEN 'Average Converting'
            WHEN wr.overall_conversion_rate >= 0.01 THEN 'Low Converting'
            ELSE 'Very Low'
        END AS conversion_tier

    FROM with_rates wr
    LEFT JOIN products p ON wr.product_id = p.product_id
)

SELECT * FROM enriched
