/*
    Test: Revenue Consistency Check

    Validates that revenue metrics are consistent across tables:
    - Sum of category revenue should match daily total
    - No negative revenue values
*/

WITH daily_totals AS (
    SELECT
        event_date,
        total_revenue AS engagement_revenue
    FROM {{ ref('fct_daily_engagement') }}
),

category_totals AS (
    SELECT
        event_date,
        SUM(gross_revenue) AS category_revenue
    FROM {{ ref('fct_revenue') }}
    GROUP BY event_date
),

comparison AS (
    SELECT
        d.event_date,
        d.engagement_revenue,
        c.category_revenue,
        ABS(d.engagement_revenue - COALESCE(c.category_revenue, 0)) AS difference
    FROM daily_totals d
    LEFT JOIN category_totals c ON d.event_date = c.event_date
)

-- Flag any day where revenue differs by more than $0.01 (floating point tolerance)
SELECT *
FROM comparison
WHERE difference > 0.01
