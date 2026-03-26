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
        s.user_id,
        s.plan_id,
        p.plan_name,
        p.billing_cycle,
        p.list_price
    FROM {{ ref('stg_subscriptions') }} s
    LEFT JOIN {{ ref('stg_plans') }} p
        ON s.plan_id = p.plan_id
    WHERE s.plan_id != 'plan_free'
      AND s.status = 'active'
)
SELECT
    a.user_id,
    a.activity_month,
    a.device_type,
    a.category,
    p.plan_id,
    p.plan_name,
    p.billing_cycle,
    p.list_price,
    a.total_watch_seconds,
    a.total_events,
    ROUND(a.total_watch_seconds / 3600.0, 2) AS watch_hours
FROM user_activity a
LEFT JOIN user_plan p ON a.user_id = p.user_id