"""
marketing_pipeline_dag.py
Orchestrates the full marketing analytics pipeline end to end:

    simulate ads ─┐
                  ├─> load (DuckDB) ─> enrich (LLM, offline) ─> dbt build ─> notify
    simulate fb  ─┘

The enrichment task runs in --offline mode: it reads the committed enrichment
fixture rather than calling the Gemini API, so the daily DAG is deterministic,
key-free, and never consumes the free-tier quota. A live enrichment that refreshes
the fixture is a separate, occasional job (see .github/workflows/live-enrichment.yml).
"""

import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ── Paths ─────────────────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INGESTION_DIR = PROJECT_ROOT / "ingestion"
ENRICHMENT_DIR = PROJECT_ROOT / "enrichment"
DBT_DIR = PROJECT_ROOT / "dbt"

# ── DAG defaults ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── Task callables ────────────────────────────────────────────────────────────

def _run(script: str, *args: str):
    """Run a project script from the repo root, streaming output to the task log."""
    result = subprocess.run(
        [sys.executable, str(script), *args],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)


def run_simulate_ads(**_):
    _run(INGESTION_DIR / "simulate_ad_data.py")


def run_simulate_feedback(**_):
    _run(INGESTION_DIR / "simulate_feedback_data.py")


def run_load(**_):
    # DuckDB is the default target; --truncate makes the load idempotent.
    _run(INGESTION_DIR / "load_to_warehouse.py", "--truncate")


def run_enrich(**_):
    # Offline: reproduce enrichment from the committed fixture (no API, no quota).
    _run(ENRICHMENT_DIR / "enrich_feedback.py", "--offline")


def notify_success(**context):
    ts = context["logical_date"].isoformat()
    print(f"[{ts}] Pipeline completed successfully")
    print(f"  DAG:          {context['dag'].dag_id}")
    print(f"  Run ID:       {context['run_id']}")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="marketing_analytics_pipeline",
    default_args=default_args,
    description="Ingest ads + feedback, LLM-enrich, transform and test (DuckDB)",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing", "dbt", "duckdb", "llm"],
) as dag:

    simulate_ads = PythonOperator(
        task_id="simulate_ads",
        python_callable=run_simulate_ads,
    )

    simulate_feedback = PythonOperator(
        task_id="simulate_feedback",
        python_callable=run_simulate_feedback,
    )

    load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=run_load,
    )

    enrich = PythonOperator(
        task_id="enrich_feedback",
        python_callable=run_enrich,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="dbt build --profiles-dir . --target dev",
        cwd=str(DBT_DIR),
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    [simulate_ads, simulate_feedback] >> load >> enrich >> dbt_build >> notify
