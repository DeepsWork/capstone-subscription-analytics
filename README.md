![Pipeline Tests](https://github.com/DeepsWork/capstone-subscription-analytics/actions/workflows/pipeline_tests.yml/badge.svg)
# Subscription Engagement and Churn Analytics Pipeline

A local batch data pipeline that transforms raw subscription data into
business-ready analytics marts using the Medallion Architecture
(Bronze → Silver → Gold).

---

## Business Questions Answered

| Question | Mart |
|----------|------|
| How many trial users convert to paid, and how long does it take? | `mart_trial_conversion` |
| Which plans and devices drive stronger retention? | `mart_retention` |
| Where do payment failures occur and how many recover in 7 days? | `mart_payment_failures` |
| Which users show high churn risk? | `mart_churn_risk` |
| How do WAU, watch time, and renewals move together weekly? | `mart_weekly_metrics` |

---

## Architecture
```
Raw Data (CSV/JSON)
      ↓
Bronze Layer — DuckDB
Ingest raw files as-is, add metadata
      ↓
Silver Layer — DuckDB
Clean, deduplicate, standardize, type cast
      ↓
Staging Layer — dbt views
Thin rename layer over Silver tables
      ↓
Gold Layer — dbt tables
5 business marts answering key questions
      ↓
data/gold_export/*.csv
Final outputs ready for analysis
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python 3.12 | Pipeline orchestration |
| DuckDB | Local Bronze + Silver processing |
| dbt + dbt-duckdb | Gold layer SQL models |
| Faker + Pandas | Synthetic data generation |
| pytest | Unit testing |
| Git | Version control |

---

## Project Structure
```
capstone_project/
├── data/
│   ├── raw/                    ← generated input files
│   ├── silver_export/          ← cleaned Silver CSVs
│   └── gold_export/            ← final mart CSVs
├── ingestion/
│   ├── generate_data.py        ← synthetic data generation
│   └── bronze_local.py         ← Bronze layer (DuckDB)
├── transformation/
│   └── silver_local.py         ← Silver layer (DuckDB)
├── dbt_project/
│   └── models/
│       ├── staging/            ← 6 staging views
│       └── marts/              ← 5 Gold mart tables
├── tests/
│   └── test_silver.py          ← 12 pytest unit tests
├── pipeline_runner.py          ← orchestrates all stages
└── README.md
```

---

## Quickstart

### 1. Clone the repo
```bash
git clone https://github.com/DeepsWork/capstone-subscription-analytics.git
cd capstone-subscription-analytics
```

### 2. Install dependencies
```bash
uv install
```

### 3. Run the full pipeline
```bash
uv run python pipeline_runner.py
```

That's it. One command runs all 5 stages:
```
Stage 1 → generates 500 users, 90 days of data
Stage 2 → loads Bronze tables into DuckDB
Stage 3 → cleans Silver tables (deduplication, type casting)
Stage 4 → builds 5 Gold marts via dbt
Stage 5 → exports Gold marts to data/gold_export/
```

---

## Running Tests

### pytest (unit tests)
```bash
uv run pytest tests/ -v
```

Expected output:
```
12 passed in 1.81s
```

### dbt tests (data quality)
```bash
cd dbt_project
uv run dbt test --profiles-dir . --select marts
```

Expected output:
```
16 passed, 0 failed
```

---

## Pipeline Layers Explained

### Bronze
- Reads raw CSV and JSON files
- Writes to DuckDB tables as-is
- Adds `_ingested_at` metadata column
- Zero transformations — preserves raw data exactly

### Silver
- Removes duplicate content events (820 duplicates removed)
- Casts all timestamps to proper types
- Standardizes status values (active/cancelled/expired)
- Fills null failure reasons with 'none'
- Exports clean CSVs to `data/silver_export/`

### Gold (dbt)
- Staging layer: 6 thin views over Silver tables
- Mart layer: 5 business tables answering key questions
- All marts materialized as tables in DuckDB

---

## Data Model

### Input files (generated)

| File | Rows | Description |
|------|------|-------------|
| users.csv | 500 | User signups with country, channel, device |
| subscriptions.csv | ~898 | Plan subscriptions with status |
| payments.csv | ~431 | Payments with failure reasons |
| content_events.json | ~17316 | Watch events (includes duplicates) |
| plans.csv | 5 | Plan reference table |
| support_cases.csv | ~303 | Customer support cases |

### Output marts

| Mart | Rows | Grain |
|------|------|-------|
| mart_trial_conversion | ~579 | One row per user trial |
| mart_retention | ~12530 | One row per user per month per device |
| mart_payment_failures | ~64 | One row per failed payment |
| mart_churn_risk | 500 | One row per user |
| mart_weekly_metrics | 14 | One row per week |

---

## Key Metric Definitions

**Churn Risk Score** — scored 0 to 3 based on:
- +1 if inactive for more than 30 days
- +1 if has at least one failed payment
- +1 if has a cancelled subscription

**Trial Conversion** — user moved from `plan_free` to any paid
plan after their trial ended

**Payment Recovery** — a failed payment followed by a successful
payment on the same subscription within 7 days

---

## Author

Deep Shah — Data Engineering Intern Capstone
Duration: 7 days | Stack: Python, DuckDB, dbt