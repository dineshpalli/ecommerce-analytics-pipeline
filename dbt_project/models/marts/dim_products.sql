{{
    config(
        materialized='table',
        tags=['marts', 'dimension']
    )
}}

/*
    Dimension: Products

    Purpose:
    - Provide a complete product dimension for analytics
    - Include performance metrics
    - Enable product analysis and recommendations

    Grain: One row per product
    Update frequency: Daily
*/

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

performance AS (
    SELECT * FROM {{ ref('int_product_performance') }}
),

final AS (
    SELECT
        -- Primary key
        p.product_id,

        -- Product attributes
        p.product_name,
        p.category,
        p.subcategory,
        p.brand,

        -- Pricing
        p.price,
        p.price_tier,

        -- Quality indicators
        p.rating,
        p.rating_tier,
        p.review_count,
        p.is_in_stock,

        -- Performance metrics
        COALESCE(pf.total_views, 0) AS total_views,
        COALESCE(pf.unique_viewers, 0) AS unique_viewers,
        COALESCE(pf.cart_adds, 0) AS total_cart_adds,
        COALESCE(pf.purchases, 0) AS total_purchases,
        COALESCE(pf.total_revenue, 0) AS total_revenue,

        -- Conversion rates
        COALESCE(pf.view_to_cart_rate, 0) AS view_to_cart_rate,
        COALESCE(pf.cart_to_purchase_rate, 0) AS cart_to_purchase_rate,
        COALESCE(pf.overall_conversion_rate, 0) AS overall_conversion_rate,
        COALESCE(pf.cart_abandonment_rate, 0) AS cart_abandonment_rate,
        COALESCE(pf.avg_order_value, 0) AS avg_order_value,

        -- Time metrics
        pf.first_interaction_date,
        pf.last_interaction_date,
        COALESCE(pf.active_days, 0) AS active_days,

        -- Performance tiers
        COALESCE(pf.revenue_tier, 'No Sales') AS revenue_tier,
        COALESCE(pf.conversion_tier, 'Very Low') AS conversion_tier,

        -- Composite scores
        CASE
            WHEN COALESCE(pf.total_revenue, 0) > 0 AND COALESCE(pf.overall_conversion_rate, 0) > 0.05
            THEN 'Star'
            WHEN COALESCE(pf.total_views, 0) > 100 AND COALESCE(pf.overall_conversion_rate, 0) <= 0.02
            THEN 'Underperformer'
            WHEN COALESCE(pf.total_views, 0) < 10
            THEN 'Low Visibility'
            ELSE 'Standard'
        END AS product_health,

        -- Metadata
        p._loaded_at,
        CURRENT_TIMESTAMP AS _updated_at

    FROM products p
    LEFT JOIN performance pf ON p.product_id = pf.product_id
)

SELECT * FROM final
