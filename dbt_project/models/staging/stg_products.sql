{{
    config(
        materialized='view',
        tags=['staging', 'products']
    )
}}

/*
    Staging model for product catalog

    Purpose:
    - Clean and standardize product data
    - Add price tiers for analysis
    - Prepare for dimension table

    Grain: One row per product
*/

WITH source AS (
    SELECT * FROM {{ source('seeds', 'products') }}
),

cleaned AS (
    SELECT
        -- Primary key
        product_id,

        -- Product attributes
        product_name,
        COALESCE(category, 'Uncategorized') AS category,
        COALESCE(subcategory, 'Other') AS subcategory,
        COALESCE(brand, 'Unknown Brand') AS brand,

        -- Pricing
        CAST(price AS DECIMAL(10, 2)) AS price,
        CASE
            WHEN price < 25 THEN 'Budget'
            WHEN price < 100 THEN 'Mid-Range'
            WHEN price < 500 THEN 'Premium'
            ELSE 'Luxury'
        END AS price_tier,

        -- Product quality metrics
        COALESCE(CAST(rating AS DECIMAL(2, 1)), 0) AS rating,
        CASE
            WHEN rating >= 4.5 THEN 'Excellent'
            WHEN rating >= 4.0 THEN 'Good'
            WHEN rating >= 3.0 THEN 'Average'
            ELSE 'Below Average'
        END AS rating_tier,
        COALESCE(review_count, 0) AS review_count,

        -- Availability
        COALESCE(in_stock, TRUE) AS is_in_stock,

        -- Metadata
        CURRENT_TIMESTAMP AS _loaded_at

    FROM source
    WHERE product_id IS NOT NULL
)

SELECT * FROM cleaned
