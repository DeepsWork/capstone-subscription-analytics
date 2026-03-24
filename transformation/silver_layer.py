# ── Silver Layer ──────────────────────────────────────────
# Purpose : Read Bronze Delta tables, apply cleaning and
#           standardization, write trusted Silver tables.
#           No business logic here — just data quality.
# ─────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, when, lit,
    lower, upper, trim, count, row_number
)
from pyspark.sql.window import Window

BRONZE_PATH = "/Volumes/workspace/capstone/bronze"
SILVER_PATH = "/Volumes/workspace/capstone/silver"

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ── helpers ───────────────────────────────────────────────
def read_bronze(table_name):
    path = f"{BRONZE_PATH}/{table_name}"
    df = spark.read.format("delta").load(path)
    print(f"  {table_name:<25} → {df.count()} rows read from bronze")
    return df

def write_silver(df, table_name):
    path = f"{SILVER_PATH}/{table_name}"
    (df.write
        .format("delta")
        .mode("overwrite")
        .save(path))
    count = spark.read.format("delta").load(path).count()
    print(f"  {table_name:<25} → {count} rows written to silver")

# ═══════════════════════════════════════════════════════════
# 1. plans — cast price, trim name
# ═══════════════════════════════════════════════════════════
print("\n── plans ──")
plans_df = read_bronze("plans")

plans_silver = (plans_df
    .withColumn("list_price",
        col("list_price").cast("double"))
    .withColumn("plan_name",
        trim(lower(col("plan_name"))))
    .withColumn("billing_cycle",
        trim(lower(col("billing_cycle"))))
    .drop("_ingested_at", "_source_file")
)

write_silver(plans_silver, "plans")

# ═══════════════════════════════════════════════════════════
# 2. users — cast timestamp, trim strings
# ═══════════════════════════════════════════════════════════
print("\n── users ──")
users_df = read_bronze("users")

users_silver = (users_df
    .withColumn("signup_ts",
        to_timestamp(col("signup_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("country",
        trim(upper(col("country"))))
    .withColumn("acquisition_channel",
        trim(lower(col("acquisition_channel"))))
    .withColumn("device_preference",
        trim(lower(col("device_preference"))))
    .filter(col("user_id").isNotNull())
    .filter(col("signup_ts").isNotNull())
    .drop("_ingested_at", "_source_file")
)

write_silver(users_silver, "users")

# ═══════════════════════════════════════════════════════════
# 3. subscriptions — cast timestamps, standardize status
# ═══════════════════════════════════════════════════════════
print("\n── subscriptions ──")
subs_df = read_bronze("subscriptions")

subs_silver = (subs_df
    .withColumn("start_ts",
        to_timestamp(col("start_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("end_ts",
        to_timestamp(col("end_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("status",
        trim(lower(col("status"))))
    .withColumn("status",
        when(col("status").isin("active", "cancelled", "expired"),
             col("status"))
        .otherwise(lit("unknown")))
    .withColumn("auto_renew_flag",
        col("auto_renew_flag").cast("boolean"))
    .filter(col("subscription_id").isNotNull())
    .filter(col("user_id").isNotNull())
    .drop("_ingested_at", "_source_file")
)

write_silver(subs_silver, "subscriptions")

# ═══════════════════════════════════════════════════════════
# 4. payments — cast timestamp, fill null failure_reason
# ═══════════════════════════════════════════════════════════
print("\n── payments ──")
payments_df = read_bronze("payments")

payments_silver = (payments_df
    .withColumn("payment_ts",
        to_timestamp(col("payment_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("amount",
        col("amount").cast("double"))
    .withColumn("payment_status",
        trim(lower(col("payment_status"))))
    .withColumn("failure_reason",
        when(col("failure_reason").isNull(), lit("none"))
        .otherwise(trim(lower(col("failure_reason")))))
    .filter(col("payment_id").isNotNull())
    .filter(col("payment_ts").isNotNull())
    .drop("_ingested_at", "_source_file")
)

write_silver(payments_silver, "payments")

# ═══════════════════════════════════════════════════════════
# 5. content_events — DEDUPLICATE, cast timestamp
#    Duplicate = same user_id + content_id + event_ts + event_type
# ═══════════════════════════════════════════════════════════
print("\n── content_events ──")
events_df = read_bronze("content_events")

dedup_window = Window.partitionBy(
    "user_id", "content_id", "event_ts", "event_type"
).orderBy("event_ts")

events_silver = (events_df
    .withColumn("event_ts",
        to_timestamp(col("event_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("watch_seconds",
        col("watch_seconds").cast("integer"))
    .withColumn("event_type",
        trim(lower(col("event_type"))))
    .withColumn("device_type",
        trim(lower(col("device_type"))))
    .filter(col("user_id").isNotNull())
    .filter(col("event_ts").isNotNull())
    .filter(col("watch_seconds") > 0)
    .withColumn("rn", row_number().over(dedup_window))
    .filter(col("rn") == 1)
    .drop("rn", "_ingested_at", "_source_file")
)

write_silver(events_silver, "content_events")

# ═══════════════════════════════════════════════════════════
# 6. support_cases — cast timestamp, standardize status
# ═══════════════════════════════════════════════════════════
print("\n── support_cases ──")
support_df = read_bronze("support_cases")

support_silver = (support_df
    .withColumn("case_ts",
        to_timestamp(col("case_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("issue_type",
        trim(lower(col("issue_type"))))
    .withColumn("resolution_status",
        trim(lower(col("resolution_status"))))
    .withColumn("resolution_status",
        when(col("resolution_status")
             .isin("resolved", "pending", "escalated"),
             col("resolution_status"))
        .otherwise(lit("unknown")))
    .filter(col("case_id").isNotNull())
    .drop("_ingested_at", "_source_file")
)

write_silver(support_silver, "support_cases")

print("\nSilver layer complete.\n")


## Run it on Databricks

# Create a new notebook, paste the full script and run. Expected output:
# ```
# ── plans ──
#   plans          → 5 rows read from bronze
#   plans          → 5 rows written to silver

# ── users ──
#   users          → 500 rows read from bronze
#   users          → 500 rows written to silver

# ── subscriptions ──
#   subscriptions  → 898 rows read from bronze
#   subscriptions  → 898 rows written to silver

# ── payments ──
#   payments       → 431 rows read from bronze
#   payments       → 431 rows written to silver

# ── content_events ──
#   content_events → 17316 rows read from bronze
#   content_events → ~16500 rows written to silver  ← fewer due to dedup

# ── support_cases ──
#   support_cases  → 303 rows read from bronze
#   support_cases  → 303 rows written to silver

# Silver layer complete.
