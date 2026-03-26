# ── Pipeline Runner ───────────────────────────────────────
# Subscription Analytics Pipeline — Local Batch
#
# Usage:  python pipeline_runner.py
#
# Stages:
#   1 → Raw data generation
#   2 → Bronze layer (DuckDB)
#   3 → Silver layer (DuckDB)
#   4 → Gold layer   (dbt)
#   5 → Export Gold to CSV
# ─────────────────────────────────────────────────────────

import subprocess
import sys
import time
import duckdb
from pathlib import Path

BASE = Path(__file__).parent

# ── import stages ─────────────────────────────────────────
import importlib.util
import sys
from pathlib import Path

BASE = Path(__file__).parent
sys.path.insert(0, str(BASE))

# dynamically load generate_data from data folder
spec = importlib.util.spec_from_file_location(
    "generate_data",
    BASE / "ingestion" / "generate_data.py"
)
# spec = importlib.util.spec_from_file_location(
#     "generate_data",
#     BASE / "data" / "generate_data.py"
# )
gen_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gen_module)
generate = gen_module.main

from ingestion.bronze_local      import run_bronze
from transformation.silver_local import run_silver

# ── stage functions ───────────────────────────────────────
def run_stage(label, fn=None, cmd=None):
    print(f"\n{'─'*50}")
    print(f"  {label}")
    print(f"{'─'*50}")
    start = time.time()
    try:
        if fn:
            fn()
        elif cmd:
            subprocess.run(cmd, shell=True, check=True,
                           cwd=str(BASE / "dbt_project"))
        elapsed = round(time.time() - start, 1)
        print(f"\n  DONE in {elapsed}s")
    except Exception as e:
        print(f"\n  FAILED: {e}")
        sys.exit(1)

def export_gold():
    con    = duckdb.connect(str(BASE / 'capstone.duckdb'))
    EXPORT = BASE / 'data' / 'gold_export'
    EXPORT.mkdir(parents=True, exist_ok=True)
    marts  = [
        'mart_trial_conversion',
        'mart_retention',
        'mart_payment_failures',
        'mart_churn_risk',
        'mart_weekly_metrics'
    ]
    for mart in marts:
        df = con.execute(f'SELECT * FROM {mart}').df()
        df.to_csv(EXPORT / f'{mart}.csv', index=False)
        print(f'  {mart:<30} → {len(df)} rows')
    con.close()

# ── run ───────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n" + "="*50)
    print("  SUBSCRIPTION ANALYTICS PIPELINE")
    print("  Starting...")
    print("="*50)

    run_stage("Stage 1 — Data generation",    fn=generate)
    run_stage("Stage 2 — Bronze layer",       fn=run_bronze)
    run_stage("Stage 3 — Silver layer",       fn=run_silver)
    run_stage("Stage 4 — Gold layer (dbt)",
              cmd="uv run dbt run --profiles-dir .")
    run_stage("Stage 5 — Export Gold to CSV", fn=export_gold)

    print("\n" + "="*50)
    print("  PIPELINE COMPLETE")
    print("  All 5 marts ready in data/gold_export/")
    print("="*50 + "\n")