/*
    Test: Funnel Integrity Check

    Validates that funnel progression follows logical order:
    - Users at purchase <= Users at checkout
    - Users at checkout <= Users at cart
    - Users at cart <= Users at product
    - Users at product <= Users at site
*/

WITH funnel_check AS (
    SELECT
        event_date,
        users_at_site,
        users_at_product,
        users_at_cart,
        users_at_checkout,
        users_at_purchase,

        -- Check that each stage has fewer or equal users than previous
        CASE
            WHEN users_at_product > users_at_site THEN 'FAIL: product > site'
            WHEN users_at_cart > users_at_product THEN 'FAIL: cart > product'
            WHEN users_at_checkout > users_at_cart THEN 'FAIL: checkout > cart'
            WHEN users_at_purchase > users_at_checkout THEN 'FAIL: purchase > checkout'
            ELSE 'PASS'
        END AS funnel_check_result

    FROM {{ ref('fct_funnel') }}
)

SELECT *
FROM funnel_check
WHERE funnel_check_result != 'PASS'
