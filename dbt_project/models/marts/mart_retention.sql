WITH user_activity AS (
    SELECT
        e.user_id,
        DATE_TRUNC('month', e.event_ts) AS activity_month,
        e.device_type,
        e.category,
        SUM(e.watch_seconds)            AS total_watch_seconds,
        COUNT(*)                        AS total_events
    FROM {{ ref('stg_content_events') }} e
    GROUP BY 1, 2, 3, 4
),
user_plan AS (
    SELECT DISTINCT
        user_id,
        plan_id
    FROM {{ ref('stg_subscriptions') }}
    WHERE plan_id != 'plan_free'
      AND status = 'active'
)
SELECT
    a.user_id,
    a.activity_month,
    a.device_type,
    a.category,
    p.plan_id,
    a.total_watch_seconds,
    a.total_events,
    ROUND(a.total_watch_seconds / 3600.0, 2) AS watch_hours
FROM user_activity a
LEFT JOIN user_plan p ON a.user_id = p.user_id