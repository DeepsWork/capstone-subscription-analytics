# ── Bronze Local (DuckDB) ─────────────────────────────────
# Purpose : Read raw CSV/JSON files into DuckDB as bronze
#           tables. Zero transformations — raw as-is.
#           Adds _ingested_at metadata column.
# ─────────────────────────────────────────────────────────

import duckdb
from pathlib import Path
from datetime import datetime

RAW_PATH = Path(__file__).parent.parent / "data" / "raw"
DB_PATH  = Path(__file__).parent.parent / "capstone.duckdb"

def run_bronze():
    con = duckdb.connect(str(DB_PATH))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n── Bronze Layer (DuckDB) ──")

    # ── plans ─────────────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_plans AS
        SELECT *, '{now}' AS _ingested_at, 'plans.csv' AS _source_file
        FROM read_csv_auto('{RAW_PATH}/plans.csv', header=true)
    """)
    print(f"  bronze_plans          → {con.execute('SELECT COUNT(*) FROM bronze_plans').fetchone()[0]} rows")

    # ── users ─────────────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_users AS
        SELECT *, '{now}' AS _ingested_at, 'users.csv' AS _source_file
        FROM read_csv_auto('{RAW_PATH}/users.csv', header=true)
    """)
    print(f"  bronze_users          → {con.execute('SELECT COUNT(*) FROM bronze_users').fetchone()[0]} rows")

    # ── subscriptions ─────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_subscriptions AS
        SELECT *, '{now}' AS _ingested_at, 'subscriptions.csv' AS _source_file
        FROM read_csv_auto('{RAW_PATH}/subscriptions.csv', header=true)
    """)
    print(f"  bronze_subscriptions  → {con.execute('SELECT COUNT(*) FROM bronze_subscriptions').fetchone()[0]} rows")

    # ── payments ──────────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_payments AS
        SELECT *, '{now}' AS _ingested_at, 'payments.csv' AS _source_file
        FROM read_csv_auto('{RAW_PATH}/payments.csv', header=true)
    """)
    print(f"  bronze_payments       → {con.execute('SELECT COUNT(*) FROM bronze_payments').fetchone()[0]} rows")

    # ── content_events (JSON) ─────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_content_events AS
        SELECT *, '{now}' AS _ingested_at, 'content_events.json' AS _source_file
        FROM read_json_auto('{RAW_PATH}/content_events.json')
    """)
    print(f"  bronze_content_events → {con.execute('SELECT COUNT(*) FROM bronze_content_events').fetchone()[0]} rows")

    # ── support_cases ─────────────────────────────────────
    con.execute(f"""
        CREATE OR REPLACE TABLE bronze_support_cases AS
        SELECT *, '{now}' AS _ingested_at, 'support_cases.csv' AS _source_file
        FROM read_csv_auto('{RAW_PATH}/support_cases.csv', header=true)
    """)
    print(f"  bronze_support_cases  → {con.execute('SELECT COUNT(*) FROM bronze_support_cases').fetchone()[0]} rows")

    con.close()
    print("  Bronze complete.\n")

if __name__ == "__main__":
    run_bronze()
