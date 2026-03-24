WITH last_activity AS (
    SELECT
        user_id,
        MAX(event_ts) AS last_event_ts
    FROM silver_content_events
    GROUP BY 1
),
failed_payments AS (
    SELECT
        s.user_id,
        COUNT(*) AS failed_payment_count
    FROM silver_payments p
    JOIN silver_subscriptions s
        ON p.subscription_id = s.subscription_id
    WHERE p.payment_status = 'failed'
    GROUP BY 1
),
downgrades AS (
    SELECT DISTINCT user_id
    FROM silver_subscriptions
    WHERE status = 'cancelled'
),
churn_signals AS (
    SELECT
        u.user_id,
        u.country,
        u.acquisition_channel,
        la.last_event_ts,
        DATEDIFF('day', la.last_event_ts, CURRENT_TIMESTAMP) AS days_inactive,
        COALESCE(fp.failed_payment_count, 0)                 AS failed_payment_count,
        CASE WHEN d.user_id IS NOT NULL THEN true ELSE false END AS has_cancellation
    FROM silver_users u
    LEFT JOIN last_activity la   ON u.user_id = la.user_id
    LEFT JOIN failed_payments fp ON u.user_id = fp.user_id
    LEFT JOIN downgrades d       ON u.user_id = d.user_id
)
SELECT
    user_id,
    country,
    acquisition_channel,
    last_event_ts,
    days_inactive,
    failed_payment_count,
    has_cancellation,
    (CASE WHEN days_inactive > 30       THEN 1 ELSE 0 END +
     CASE WHEN failed_payment_count > 0 THEN 1 ELSE 0 END +
     CASE WHEN has_cancellation         THEN 1 ELSE 0 END
    ) AS churn_risk_score,
    CASE
        WHEN (CASE WHEN days_inactive > 30       THEN 1 ELSE 0 END +
              CASE WHEN failed_payment_count > 0 THEN 1 ELSE 0 END +
              CASE WHEN has_cancellation         THEN 1 ELSE 0 END
             ) = 3 THEN 'high'
        WHEN (CASE WHEN days_inactive > 30       THEN 1 ELSE 0 END +
              CASE WHEN failed_payment_count > 0 THEN 1 ELSE 0 END +
              CASE WHEN has_cancellation         THEN 1 ELSE 0 END
             ) = 2 THEN 'medium'
        ELSE 'low'
    END AS churn_risk_label
FROM churn_signals