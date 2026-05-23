"""
export_marts.py
Export the Gold (MARTS) tables from DuckDB to Parquet so Power BI can read them
without a live warehouse connection. Power BI connects to the dashboard/exports/
folder (Get Data -> Parquet, or the folder connector).

This replaces the original Snowflake -> Power BI connector path: the warehouse is
now local DuckDB, so we serve the marts as files instead.

Usage:
    python dashboard/export_marts.py
"""

import os
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = Path(os.environ.get("DUCKDB_PATH", str(PROJECT_ROOT / "MARKETING_ANALYTICS.duckdb")))
EXPORT_DIR = PROJECT_ROOT / "dashboard" / "exports"

# Gold tables to serve to the dashboard.
MARTS = [
    "fct_ad_spend",
    "fct_channel_daily",
    "fct_campaign_summary",
    "dim_campaigns",
    "fct_feedback",
    "fct_feedback_themes",
    "fct_campaign_performance",
]


def main() -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    try:
        print(f"\nExporting MARTS from {DUCKDB_PATH.name} -> {EXPORT_DIR}\n")
        for table in MARTS:
            out = EXPORT_DIR / f"{table}.parquet"
            con.execute(
                f"COPY (SELECT * FROM MARTS.{table}) TO '{out.as_posix()}' (FORMAT PARQUET)"
            )
            rows = con.execute(f"SELECT count(*) FROM MARTS.{table}").fetchone()[0]
            print(f"  OK {table:26} {rows:>6,} rows -> {out.name}")
        print(f"\nDone. {len(MARTS)} tables exported to {EXPORT_DIR}\n")
    finally:
        con.close()


if __name__ == "__main__":
    main()
