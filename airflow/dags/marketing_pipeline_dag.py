"""
marketing_pipeline_dag.py
Orchestrates the full marketing analytics pipeline:
simulate data -> load to Snowflake -> dbt run -> dbt test -> notify
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
DBT_DIR = PROJECT_ROOT / "dbt"

# ── DAG defaults ──────────────────────────────────────────────────────────────

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── Task callables ────────────────────────────────────────────────────────────


def run_simulate_data(**context):
    """Generate synthetic ad data CSVs."""
    result = subprocess.run(
        [sys.executable, str(INGESTION_DIR / "simulate_ad_data.py")],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    print(result.stdout)


def run_load_to_snowflake(**context):
    """Load CSVs into Snowflake RAW schema with truncate."""
    result = subprocess.run(
        [sys.executable, str(INGESTION_DIR / "load_to_snowflake.py"), "--truncate"],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=True,
    )
    print(result.stdout)


def notify_success(**context):
    """Log a success summary."""
    ts = context["logical_date"].isoformat()
    print(f"[{ts}] Pipeline completed successfully")
    print(f"  DAG:        {context['dag'].dag_id}")
    print(f"  Run ID:     {context['run_id']}")
    print(f"  Logical date: {ts}")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="marketing_analytics_pipeline",
    default_args=default_args,
    description="End-to-end marketing analytics: ingest, load, transform, test",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["marketing", "dbt", "snowflake"],
) as dag:

    simulate_data = PythonOperator(
        task_id="simulate_data",
        python_callable=run_simulate_data,
    )

    load_to_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=run_load_to_snowflake,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select staging marts",
        cwd=str(DBT_DIR),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test",
        cwd=str(DBT_DIR),
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    simulate_data >> load_to_snowflake >> dbt_run >> dbt_test >> notify
