WITH weekly_activity AS (
    SELECT
        DATE_TRUNC('week', event_ts)          AS week_start,
        COUNT(DISTINCT user_id)               AS weekly_active_users,
        SUM(watch_seconds)                    AS total_watch_seconds,
        ROUND(SUM(watch_seconds) / 3600.0, 2) AS total_watch_hours
    FROM {{ ref('stg_content_events') }}
    GROUP BY 1
),
weekly_renewals AS (
    SELECT
        DATE_TRUNC('week', start_ts) AS week_start,
        COUNT(*)                     AS renewals
    FROM {{ ref('stg_subscriptions') }}
    WHERE auto_renew_flag = true
      AND status = 'active'
    GROUP BY 1
)
SELECT
    a.week_start,
    a.weekly_active_users,
    a.total_watch_seconds,
    a.total_watch_hours,
    COALESCE(r.renewals, 0) AS renewals
FROM weekly_activity a
LEFT JOIN weekly_renewals r ON a.week_start = r.week_start
ORDER BY 1