WITH failures AS (
    SELECT
        payment_id,
        subscription_id,
        payment_ts   AS failed_at,
        amount,
        failure_reason
    FROM {{ ref('stg_payments') }}
    WHERE payment_status = 'failed'
),
recoveries AS (
    SELECT
        p.subscription_id,
        MIN(p.payment_ts) AS recovered_at
    FROM {{ ref('stg_payments') }} p
    INNER JOIN failures f
        ON p.subscription_id = f.subscription_id
        AND p.payment_status = 'success'
        AND p.payment_ts > f.failed_at
        AND p.payment_ts <= f.failed_at + INTERVAL '7 days'
    GROUP BY 1
)
SELECT
    f.payment_id,
    f.subscription_id,
    f.failed_at,
    f.amount,
    f.failure_reason,
    r.recovered_at,
    CASE
        WHEN r.recovered_at IS NOT NULL THEN true
        ELSE false
    END AS recovered_within_7_days,
    DATEDIFF('day', f.failed_at, r.recovered_at) AS days_to_recovery
FROM failures f
LEFT JOIN recoveries r ON f.subscription_id = r.subscription_id