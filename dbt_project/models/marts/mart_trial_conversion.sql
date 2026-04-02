-- WITH trials AS (
--     SELECT
--         user_id,
--         start_ts AS trial_start,
--         end_ts   AS trial_end
--     FROM {{ ref('stg_subscriptions') }}
--     WHERE plan_id = 'plan_free'
-- ),
-- paid AS (
--     SELECT
--         user_id,
--         plan_id,
--         start_ts AS paid_start
--     FROM {{ ref('stg_subscriptions') }}
--     WHERE plan_id != 'plan_free'
-- ),
-- joined AS (
--     SELECT
--         t.user_id,
--         t.trial_start,
--         t.trial_end,
--         p.plan_id        AS converted_plan,
--         p.paid_start,
--         DATEDIFF('day', t.trial_end, p.paid_start) AS days_to_convert
--     FROM trials t
--     LEFT JOIN paid p
--         ON t.user_id = p.user_id
--         AND p.paid_start >= t.trial_start
-- )
-- SELECT
--     user_id,
--     trial_start,
--     trial_end,
--     converted_plan,
--     paid_start,
--     days_to_convert,
--     CASE
--         WHEN converted_plan IS NOT NULL THEN 'converted'
--         ELSE 'not_converted'
--     END AS conversion_status
-- FROM joined

WITH trials AS (
    SELECT
        user_id,
        start_ts AS trial_start,
        end_ts   AS trial_end
    FROM {{ ref('stg_subscriptions') }}
    WHERE plan_id = 'plan_free'
),
paid AS (
    SELECT
        user_id,
        plan_id,
        start_ts AS paid_start
    FROM {{ ref('stg_subscriptions') }}
    WHERE plan_id != 'plan_free'
),
joined AS (
    SELECT
        t.user_id,
        t.trial_start,
        t.trial_end,
        p.plan_id        AS converted_plan,
        p.paid_start,
        DATEDIFF('day', t.trial_end, p.paid_start) AS days_to_convert
    FROM trials t
    LEFT JOIN paid p
        ON t.user_id = p.user_id
        AND p.paid_start >= t.trial_start
)
SELECT
    user_id,
    trial_start,
    trial_end,
    converted_plan,
    paid_start,
    days_to_convert,
    CASE
        WHEN converted_plan IS NOT NULL THEN 'converted'
        ELSE 'not_converted'
    END AS conversion_status
FROM joined
