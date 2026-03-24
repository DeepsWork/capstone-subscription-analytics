# ── Bronze Layer ──────────────────────────────────────────
# Purpose : Read raw files from DBFS and write as Delta
#           tables. Zero transformations — raw as-is.
#           Only adds ingestion metadata columns.
# ─────────────────────────────────────────────────────────

from pyspark.sql.functions import current_timestamp, lit

RAW_PATH    = "dbfs:/FileStore/capstone/raw"
BRONZE_PATH = "dbfs:/FileStore/capstone/bronze"

# ── helpers ───────────────────────────────────────────────
def add_metadata(df, source_file):
    return (df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(source_file))
    )

def write_bronze(df, table_name):
    path = f"{BRONZE_PATH}/{table_name}"
    (df.write
        .format("delta")
        .mode("overwrite")
        .save(path))
    count = spark.read.format("delta").load(path).count()
    print(f"  {table_name:<25} → {count} rows")

# ═══════════════════════════════════════════════════════════
# 1. plans
# ═══════════════════════════════════════════════════════════
plans_df = spark.read.csv(
    f"{RAW_PATH}/plans.csv",
    header=True,
    inferSchema=True
)
plans_df = add_metadata(plans_df, "plans.csv")
write_bronze(plans_df, "plans")

# ═══════════════════════════════════════════════════════════
# 2. users
# ═══════════════════════════════════════════════════════════
users_df = spark.read.csv(
    f"{RAW_PATH}/users.csv",
    header=True,
    inferSchema=True
)
users_df = add_metadata(users_df, "users.csv")
write_bronze(users_df, "users")

# ═══════════════════════════════════════════════════════════
# 3. subscriptions
# ═══════════════════════════════════════════════════════════
subs_df = spark.read.csv(
    f"{RAW_PATH}/subscriptions.csv",
    header=True,
    inferSchema=True
)
subs_df = add_metadata(subs_df, "subscriptions.csv")
write_bronze(subs_df, "subscriptions")

# ═══════════════════════════════════════════════════════════
# 4. payments
# ═══════════════════════════════════════════════════════════
payments_df = spark.read.csv(
    f"{RAW_PATH}/payments.csv",
    header=True,
    inferSchema=True
)
payments_df = add_metadata(payments_df, "payments.csv")
write_bronze(payments_df, "payments")

# ═══════════════════════════════════════════════════════════
# 5. content_events (JSON)
# ═══════════════════════════════════════════════════════════
events_df = spark.read.json(
    f"{RAW_PATH}/content_events.json"
)
events_df = add_metadata(events_df, "content_events.json")
write_bronze(events_df, "content_events")

# ═══════════════════════════════════════════════════════════
# 6. support_cases
# ═══════════════════════════════════════════════════════════
support_df = spark.read.csv(
    f"{RAW_PATH}/support_cases.csv",
    header=True,
    inferSchema=True
)
support_df = add_metadata(support_df, "support_cases.csv")
write_bronze(support_df, "support_cases")

print("\nBronze layer complete.\n")
