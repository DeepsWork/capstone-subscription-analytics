# ── Silver Layer Tests ────────────────────────────────────
# Tests that verify Silver transformation logic is correct.
# Run with: uv run pytest tests/ -v
# ─────────────────────────────────────────────────────────

import pytest
import duckdb
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "capstone.duckdb"

@pytest.fixture
def con():
    """Connect to DuckDB for each test."""
    conn = duckdb.connect(str(DB_PATH))
    yield conn
    conn.close()

# ── Test 1: Silver tables exist ───────────────────────────
def test_silver_tables_exist(con):
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name LIKE 'silver_%'
    """).fetchall()
    table_names = [t[0] for t in tables]
    assert "silver_users"          in table_names
    assert "silver_subscriptions"  in table_names
    assert "silver_payments"       in table_names
    assert "silver_content_events" in table_names
    assert "silver_plans"          in table_names
    assert "silver_support_cases"  in table_names

# ── Test 2: No null user_ids in silver_users ──────────────
def test_no_null_user_ids(con):
    result = con.execute("""
        SELECT COUNT(*) FROM silver_users
        WHERE user_id IS NULL
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} null user_ids in silver_users"

# ── Test 3: Deduplication reduced content_events ─────────
def test_deduplication_reduced_rows(con):
    bronze = con.execute(
        "SELECT COUNT(*) FROM bronze_content_events"
    ).fetchone()[0]
    silver = con.execute(
        "SELECT COUNT(*) FROM silver_content_events"
    ).fetchone()[0]
    assert silver < bronze, \
        "Silver should have fewer rows than Bronze after dedup"

# ── Test 4: No duplicate events in silver ─────────────────
def test_no_duplicate_events(con):
    result = con.execute("""
        SELECT COUNT(*) FROM (
            SELECT user_id, content_id, event_ts, event_type,
                   COUNT(*) AS cnt
            FROM silver_content_events
            GROUP BY 1, 2, 3, 4
            HAVING cnt > 1
        )
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} duplicate events in silver_content_events"

# ── Test 5: Subscription status only valid values ─────────
def test_subscription_status_values(con):
    result = con.execute("""
        SELECT COUNT(*) FROM silver_subscriptions
        WHERE status NOT IN
            ('active', 'cancelled', 'expired', 'unknown')
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} invalid status values"

# ── Test 6: No zero or negative watch seconds ─────────────
def test_no_negative_watch_seconds(con):
    result = con.execute("""
        SELECT COUNT(*) FROM silver_content_events
        WHERE watch_seconds <= 0
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} rows with zero or negative watch_seconds"

# ── Test 7: Payment status only valid values ──────────────
def test_payment_status_values(con):
    result = con.execute("""
        SELECT COUNT(*) FROM silver_payments
        WHERE payment_status NOT IN ('success', 'failed')
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} invalid payment_status values"

# ── Test 8: All users have valid signup timestamps ────────
def test_signup_timestamps_not_null(con):
    result = con.execute("""
        SELECT COUNT(*) FROM silver_users
        WHERE signup_ts IS NULL
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} null signup timestamps"

# ── Test 9: Bronze tables exist ───────────────────────────
def test_bronze_tables_exist(con):
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name LIKE 'bronze_%'
    """).fetchall()
    table_names = [t[0] for t in tables]
    assert "bronze_users"          in table_names
    assert "bronze_subscriptions"  in table_names
    assert "bronze_payments"       in table_names
    assert "bronze_content_events" in table_names

# ── Test 10: Gold marts exist ─────────────────────────────
def test_gold_marts_exist(con):
    tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_name LIKE 'mart_%'
    """).fetchall()
    table_names = [t[0] for t in tables]
    assert "mart_churn_risk"       in table_names
    assert "mart_trial_conversion" in table_names
    assert "mart_payment_failures" in table_names
    assert "mart_retention"        in table_names
    assert "mart_weekly_metrics"   in table_names

# ── Test 11: Churn risk score is 0 to 3 ──────────────────
def test_churn_risk_score_range(con):
    result = con.execute("""
        SELECT COUNT(*) FROM mart_churn_risk
        WHERE churn_risk_score NOT IN (0, 1, 2, 3)
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} invalid churn risk scores"

# ── Test 12: Payment failure amounts are positive ─────────
def test_payment_amounts_positive(con):
    result = con.execute("""
        SELECT COUNT(*) FROM silver_payments
        WHERE amount <= 0
    """).fetchone()[0]
    assert result == 0, \
        f"Found {result} payments with zero or negative amount"