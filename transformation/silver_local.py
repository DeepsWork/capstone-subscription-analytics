# ── Silver Local (DuckDB) ─────────────────────────────────
# Purpose : Read bronze DuckDB tables, clean and standardize,
#           write trusted silver tables. No business logic.
# ─────────────────────────────────────────────────────────

import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "capstone.duckdb"

def run_silver():
    con = duckdb.connect(str(DB_PATH))

    print("── Silver Layer (DuckDB) ──")

    # ── plans ─────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_plans AS
        SELECT
            plan_id,
            LOWER(TRIM(plan_name))     AS plan_name,
            LOWER(TRIM(billing_cycle)) AS billing_cycle,
            CAST(list_price AS DOUBLE) AS list_price
        FROM bronze_plans
    """)
    print(f"  silver_plans          → {con.execute('SELECT COUNT(*) FROM silver_plans').fetchone()[0]} rows")

    # ── users ─────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_users AS
        SELECT
            user_id,
            CAST(signup_ts AS TIMESTAMP)          AS signup_ts,
            UPPER(TRIM(country))                  AS country,
            LOWER(TRIM(acquisition_channel))      AS acquisition_channel,
            LOWER(TRIM(device_preference))        AS device_preference
        FROM bronze_users
        WHERE user_id IS NOT NULL
          AND signup_ts IS NOT NULL
    """)
    print(f"  silver_users          → {con.execute('SELECT COUNT(*) FROM silver_users').fetchone()[0]} rows")

    # ── subscriptions ─────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_subscriptions AS
        SELECT
            subscription_id,
            user_id,
            plan_id,
            CAST(start_ts AS TIMESTAMP) AS start_ts,
            CAST(end_ts   AS TIMESTAMP) AS end_ts,
            CASE
                WHEN LOWER(TRIM(status))
                     IN ('active','cancelled','expired') 
                THEN LOWER(TRIM(status))
                ELSE 'unknown'
            END AS status,
            CAST(auto_renew_flag AS BOOLEAN) AS auto_renew_flag
        FROM bronze_subscriptions
        WHERE subscription_id IS NOT NULL
          AND user_id IS NOT NULL
    """)
    print(f"  silver_subscriptions  → {con.execute('SELECT COUNT(*) FROM silver_subscriptions').fetchone()[0]} rows")

    # ── payments ──────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_payments AS
        SELECT
            payment_id,
            subscription_id,
            CAST(payment_ts AS TIMESTAMP)  AS payment_ts,
            CAST(amount AS DOUBLE)         AS amount,
            currency,
            LOWER(TRIM(payment_status))    AS payment_status,
            COALESCE(
                LOWER(TRIM(failure_reason)),
                'none'
            )                              AS failure_reason
        FROM bronze_payments
        WHERE payment_id  IS NOT NULL
          AND payment_ts  IS NOT NULL
    """)
    print(f"  silver_payments       → {con.execute('SELECT COUNT(*) FROM silver_payments').fetchone()[0]} rows")

    # ── content_events — DEDUPLICATE ──────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_content_events AS
        WITH deduped AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, content_id, 
                                 event_ts, event_type
                    ORDER BY event_ts
                ) AS rn
            FROM bronze_content_events
            WHERE user_id       IS NOT NULL
              AND event_ts      IS NOT NULL
              AND watch_seconds  > 0
        )
        SELECT
            user_id,
            content_id,
            CAST(event_ts AS TIMESTAMP)  AS event_ts,
            LOWER(TRIM(event_type))      AS event_type,
            CAST(watch_seconds AS INT)   AS watch_seconds,
            LOWER(TRIM(device_type))     AS device_type,
            LOWER(TRIM(category))        AS category
        FROM deduped
        WHERE rn = 1
    """)
    print(f"  silver_content_events → {con.execute('SELECT COUNT(*) FROM silver_content_events').fetchone()[0]} rows")

    # ── support_cases ─────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE silver_support_cases AS
        SELECT
            case_id,
            user_id,
            CAST(case_ts AS TIMESTAMP) AS case_ts,
            LOWER(TRIM(issue_type))    AS issue_type,
            CASE
                WHEN LOWER(TRIM(resolution_status))
                     IN ('resolved','pending','escalated')
                THEN LOWER(TRIM(resolution_status))
                ELSE 'unknown'
            END AS resolution_status
        FROM bronze_support_cases
        WHERE case_id IS NOT NULL
    """)
    print(f"  silver_support_cases  → {con.execute('SELECT COUNT(*) FROM silver_support_cases').fetchone()[0]} rows")

# ── Export Silver tables as CSV ───────────────────────
    import pandas as pd
    EXPORT_PATH = Path(__file__).parent.parent / "data" / "silver_export"
    EXPORT_PATH.mkdir(parents=True, exist_ok=True)

    print("\n── Exporting Silver tables to CSV ──")
    silver_tables = [
        "silver_plans",
        "silver_users",
        "silver_subscriptions",
        "silver_payments",
        "silver_content_events",
        "silver_support_cases",
    ]

    for table in silver_tables:
        df = con.execute(f"SELECT * FROM {table}").df()
        export_file = EXPORT_PATH / f"{table}.csv"
        df.to_csv(export_file, index=False)
        print(f"  {table:<30} → {len(df)} rows → {export_file.name}")

    print("  Export complete.\n")
    con.close()
    print("  Silver complete.\n")

if __name__ == "__main__":
    run_silver()
