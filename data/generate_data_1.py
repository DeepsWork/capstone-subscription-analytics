import pandas as pd
import json
import random
import uuid
from faker import Faker
from datetime import datetime, timedelta
from pathlib import Path

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── config ────────────────────────────────────────────────
START_DATE    = datetime(2024, 12, 1)
END_DATE      = datetime(2025, 3, 1)   # 90 days
NUM_USERS     = 500
OUTPUT_DIR    = Path(__file__).parent / "raw"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── helpers ───────────────────────────────────────────────
def random_ts(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def to_ts(dt):
    return dt.strftime("%Y-%m-%d %H:%M:%S")

# ═══════════════════════════════════════════════════════════
# 1. PLANS  (small reference table)
# ═══════════════════════════════════════════════════════════
def generate_plans():
    plans = [
        {"plan_id": "plan_free",    "plan_name": "Free Trial",  "billing_cycle": "trial",   "list_price": 0.00},
        {"plan_id": "plan_basic",   "plan_name": "Basic",       "billing_cycle": "monthly", "list_price": 9.99},
        {"plan_id": "plan_standard","plan_name": "Standard",    "billing_cycle": "monthly", "list_price": 14.99},
        {"plan_id": "plan_premium", "plan_name": "Premium",     "billing_cycle": "monthly", "list_price": 19.99},
        {"plan_id": "plan_annual",  "plan_name": "Annual",      "billing_cycle": "yearly",  "list_price": 99.99},
    ]
    df = pd.DataFrame(plans)
    df.to_csv(OUTPUT_DIR / "plans.csv", index=False)
    print(f"  plans.csv          → {len(df)} rows")
    return df

# ═══════════════════════════════════════════════════════════
# 2. USERS
# ═══════════════════════════════════════════════════════════
def generate_users():
    channels  = ["organic", "paid_search", "social_media", "referral", "email"]
    devices   = ["mobile", "desktop", "tablet", "smart_tv"]
    countries = ["IN", "US", "UK", "DE", "BR", "AU", "SG", "CA"]

    rows = []
    for _ in range(NUM_USERS):
        rows.append({
            "user_id":            str(uuid.uuid4()),
            "signup_ts":          to_ts(random_ts(START_DATE, END_DATE - timedelta(days=5))),
            "country":            random.choices(countries, weights=[30,20,10,8,8,7,7,10])[0],
            "acquisition_channel":random.choices(channels,  weights=[30,25,20,15,10])[0],
            "device_preference":  random.choice(devices),
        })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "users.csv", index=False)
    print(f"  users.csv          → {len(df)} rows")
    return df

# ═══════════════════════════════════════════════════════════
# 3. SUBSCRIPTIONS  (some users have multiple = plan changes)
# ═══════════════════════════════════════════════════════════
def generate_subscriptions(users_df, plans_df):
    paid_plans = ["plan_basic", "plan_standard", "plan_premium", "plan_annual"]
    rows = []

    for _, user in users_df.iterrows():
        signup = datetime.strptime(user["signup_ts"], "%Y-%m-%d %H:%M:%S")

        # everyone starts on free trial
        trial_end = signup + timedelta(days=random.randint(7, 14))
        rows.append({
            "subscription_id": str(uuid.uuid4()),
            "user_id":         user["user_id"],
            "plan_id":         "plan_free",
            "start_ts":        to_ts(signup),
            "end_ts":          to_ts(trial_end),
            "status":          "expired",
            "auto_renew_flag": False,
        })

        # 65% convert to paid
        if random.random() < 0.65:
            plan      = random.choice(paid_plans)
            sub_start = trial_end
            sub_end   = sub_start + timedelta(days=30 if "annual" not in plan else 365)
            status    = random.choices(["active","cancelled","expired"], weights=[60,20,20])[0]

            rows.append({
                "subscription_id": str(uuid.uuid4()),
                "user_id":         user["user_id"],
                "plan_id":         plan,
                "start_ts":        to_ts(sub_start),
                "end_ts":          to_ts(sub_end),
                "status":          status,
                "auto_renew_flag": random.choice([True, False]),
            })

            # 20% of paid users upgrade/downgrade (plan change)
            if random.random() < 0.20:
                new_plan  = random.choice([p for p in paid_plans if p != plan])
                change_ts = sub_start + timedelta(days=random.randint(5, 25))
                rows.append({
                    "subscription_id": str(uuid.uuid4()),
                    "user_id":         user["user_id"],
                    "plan_id":         new_plan,
                    "start_ts":        to_ts(change_ts),
                    "end_ts":          to_ts(change_ts + timedelta(days=30)),
                    "status":          random.choices(["active","cancelled"], weights=[70,30])[0],
                    "auto_renew_flag": random.choice([True, False]),
                })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "subscriptions.csv", index=False)
    print(f"  subscriptions.csv  → {len(df)} rows")
    return df

# ═══════════════════════════════════════════════════════════
# 4. PAYMENTS  (includes failures + nulls for failure_reason)
# ═══════════════════════════════════════════════════════════
def generate_payments(subscriptions_df, plans_df):
    failure_reasons = ["insufficient_funds", "card_expired", "bank_declined", "fraud_flag"]
    plan_price      = plans_df.set_index("plan_id")["list_price"].to_dict()
    paid_subs       = subscriptions_df[subscriptions_df["plan_id"] != "plan_free"]

    rows = []
    for _, sub in paid_subs.iterrows():
        amount        = plan_price.get(sub["plan_id"], 9.99)
        payment_ts    = datetime.strptime(sub["start_ts"], "%Y-%m-%d %H:%M:%S") + timedelta(hours=random.randint(0, 3))
        is_failed     = random.random() < 0.15   # 15% failure rate

        rows.append({
            "payment_id":         str(uuid.uuid4()),
            "subscription_id":    sub["subscription_id"],
            "payment_ts":         to_ts(payment_ts),
            "amount":             amount,
            "currency":           "USD",
            "payment_status":     "failed" if is_failed else "success",
            "failure_reason":     random.choice(failure_reasons) if is_failed else None,
        })

        # recovery payment within 7 days for ~50% of failures
        if is_failed and random.random() < 0.50:
            retry_ts = payment_ts + timedelta(days=random.randint(1, 7))
            rows.append({
                "payment_id":      str(uuid.uuid4()),
                "subscription_id": sub["subscription_id"],
                "payment_ts":      to_ts(retry_ts),
                "amount":          amount,
                "currency":        "USD",
                "payment_status":  "success",
                "failure_reason":  None,
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "payments.csv", index=False)
    print(f"  payments.csv       → {len(df)} rows")
    return df

# ═══════════════════════════════════════════════════════════
# 5. CONTENT EVENTS  (includes duplicates + partial watches)
# ═══════════════════════════════════════════════════════════
def generate_content_events(users_df):
    event_types = ["play", "pause", "stop", "seek", "complete"]
    devices     = ["mobile", "desktop", "tablet", "smart_tv"]
    categories  = ["drama", "comedy", "documentary", "sports", "kids", "thriller"]

    rows = []
    active_users = users_df.sample(frac=0.75)   # 25% inactive users (no events)

    for _, user in active_users.iterrows():
        num_events = random.randint(5, 80)
        for _ in range(num_events):
            event = {
                "user_id":       user["user_id"],
                "content_id":    f"content_{random.randint(1, 200)}",
                "event_ts":      to_ts(random_ts()),
                "event_type":    random.choices(event_types, weights=[40,20,15,15,10])[0],
                "watch_seconds": random.randint(10, 5400),   # partial to full watch
                "device_type":   random.choice(devices),
                "category":      random.choice(categories),
            }
            rows.append(event)

            # inject ~5% duplicate events (same event, same timestamp)
            if random.random() < 0.05:
                rows.append(event.copy())

    with open(OUTPUT_DIR / "content_events.json", "w") as f:
        json.dump(rows, f, indent=2)

    print(f"  content_events.json→ {len(rows)} rows  ({len(rows) - len(active_users) * 1} incl. duplicates)")
    return rows

# ═══════════════════════════════════════════════════════════
# 6. SUPPORT CASES
# ═══════════════════════════════════════════════════════════
def generate_support_cases(users_df):
    issue_types  = ["billing_issue", "playback_error", "account_access", "cancellation", "plan_change"]
    resolutions  = ["resolved", "pending", "escalated"]

    rows = []
    # only ~30% of users raise support cases
    support_users = users_df.sample(frac=0.30)

    for _, user in support_users.iterrows():
        num_cases = random.randint(1, 3)
        for _ in range(num_cases):
            rows.append({
                "case_id":           str(uuid.uuid4()),
                "user_id":           user["user_id"],
                "case_ts":           to_ts(random_ts()),
                "issue_type":        random.choice(issue_types),
                "resolution_status": random.choices(resolutions, weights=[65,25,10])[0],
            })

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_DIR / "support_cases.csv", index=False)
    print(f"  support_cases.csv  → {len(df)} rows")
    return df

# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════
# if __name__ == "__main__":
#     print("\nGenerating synthetic data...\n")
#     plans_df         = generate_plans()
#     users_df         = generate_users()
#     subscriptions_df = generate_subscriptions(users_df, plans_df)
#     payments_df      = generate_payments(subscriptions_df, plans_df)
#     _                = generate_content_events(users_df)
#     _                = generate_support_cases(users_df)
#     print(f"\nAll files written to → {OUTPUT_DIR}\n")


def main():
    print("\nGenerating synthetic data...\n")
    plans_df         = generate_plans()
    users_df         = generate_users()
    subscriptions_df = generate_subscriptions(users_df, plans_df)
    payments_df      = generate_payments(subscriptions_df, plans_df)
    _                = generate_content_events(users_df)
    _                = generate_support_cases(users_df)
    print(f"\nAll files written to → {OUTPUT_DIR}\n")

if __name__ == "__main__":
    main()
