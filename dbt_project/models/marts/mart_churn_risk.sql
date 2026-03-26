WITH last_activity AS (
    SELECT
        user_id,
        MAX(event_ts) AS last_event_ts
    FROM {{ ref('stg_content_events') }}
    GROUP BY 1
),
failed_payments AS (
    SELECT
        s.user_id,
        COUNT(*) AS failed_payment_count
    FROM {{ ref('stg_payments') }} p
    JOIN {{ ref('stg_subscriptions') }} s
        ON p.subscription_id = s.subscription_id
    WHERE p.payment_status = 'failed'
    GROUP BY 1
),
cancellations AS (
    SELECT DISTINCT user_id
    FROM {{ ref('stg_subscriptions') }}
    WHERE status = 'cancelled'
),
support_cases AS (
    SELECT
        user_id,
        COUNT(*) AS support_case_count
    FROM {{ ref('stg_support_cases') }}
    GROUP BY 1
),
churn_signals AS (
    SELECT
        u.user_id,
        u.country,
        u.channel                AS acquisition_channel,
        la.last_event_ts,
        DATEDIFF('day', la.last_event_ts,
            CURRENT_TIMESTAMP)   AS days_inactive,
        COALESCE(fp.failed_payment_count, 0)
                                 AS failed_payment_count,
        COALESCE(sc.support_case_count, 0)
                                 AS support_case_count,
        CASE WHEN c.user_id IS NOT NULL
             THEN true ELSE false
        END                      AS has_cancellation
    FROM {{ ref('stg_users') }} u
    LEFT JOIN last_activity la    ON u.user_id = la.user_id
    LEFT JOIN failed_payments fp  ON u.user_id = fp.user_id
    LEFT JOIN cancellations c     ON u.user_id = c.user_id
    LEFT JOIN support_cases sc    ON u.user_id = sc.user_id
)
SELECT
    user_id,
    country,
    acquisition_channel,
    last_event_ts,
    days_inactive,
    failed_payment_count,
    support_case_count,
    has_cancellation,
    -- churn risk score: 0 to 4
    (CASE WHEN days_inactive > 30        THEN 1 ELSE 0 END +
     CASE WHEN failed_payment_count > 0  THEN 1 ELSE 0 END +
     CASE WHEN has_cancellation          THEN 1 ELSE 0 END +
     CASE WHEN support_case_count > 1    THEN 1 ELSE 0 END
    ) AS churn_risk_score,
    CASE
        WHEN (CASE WHEN days_inactive > 30        THEN 1 ELSE 0 END +
              CASE WHEN failed_payment_count > 0  THEN 1 ELSE 0 END +
              CASE WHEN has_cancellation          THEN 1 ELSE 0 END +
              CASE WHEN support_case_count > 1    THEN 1 ELSE 0 END
             ) >= 3 THEN 'high'
        WHEN (CASE WHEN days_inactive > 30        THEN 1 ELSE 0 END +
              CASE WHEN failed_payment_count > 0  THEN 1 ELSE 0 END +
              CASE WHEN has_cancellation          THEN 1 ELSE 0 END +
              CASE WHEN support_case_count > 1    THEN 1 ELSE 0 END
             ) = 2 THEN 'medium'
        ELSE 'low'
    END AS churn_risk_label
FROM churn_signals