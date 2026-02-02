/*
    Test: Session-User Relationship Integrity

    Validates that:
    - Every session belongs to a known user
    - No orphan sessions exist
*/

WITH session_users AS (
    SELECT DISTINCT user_id
    FROM {{ ref('int_sessions') }}
),

known_users AS (
    SELECT DISTINCT user_id
    FROM {{ ref('dim_users') }}
),

orphan_sessions AS (
    SELECT su.user_id
    FROM session_users su
    LEFT JOIN known_users ku ON su.user_id = ku.user_id
    WHERE ku.user_id IS NULL
)

SELECT *
FROM orphan_sessions
