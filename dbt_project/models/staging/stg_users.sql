SELECT
    user_id,
    signup_ts,
    country,
    acquisition_channel  AS channel,
    device_preference    AS device
FROM silver_users