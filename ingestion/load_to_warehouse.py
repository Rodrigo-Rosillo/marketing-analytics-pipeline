"""
load_to_warehouse.py
Bulk-loads the raw CSVs from data/raw/ into the Bronze (RAW) schema of either
DuckDB (default, local/CI) or Snowflake (production).

The two engines share one medallion model; only the load mechanics differ:
  - DuckDB:    read_csv_auto into a local file database (no account, no secrets)
  - Snowflake: PUT + COPY INTO via the snowflake connector

Usage:
    python ingestion/load_to_warehouse.py                       # DuckDB, append
    python ingestion/load_to_warehouse.py --truncate            # DuckDB, reload
    python ingestion/load_to_warehouse.py --target snowflake --truncate
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# ── Configuration ─────────────────────────────────────────────────────────────

# Maps each CSV filename to its target RAW table.
CSV_TABLE_MAP: dict[str, str] = {
    "meta_ads_2024.csv":   "META_ADS",
    "google_ads_2024.csv": "GOOGLE_ADS",
    "tiktok_ads_2024.csv": "TIKTOK_ADS",
}

# Columns present in the CSVs (everything except the _loaded_at audit column,
# which each engine fills with a DEFAULT on insert).
CSV_COLUMNS = [
    "date", "channel", "campaign_id", "campaign_name", "objective",
    "ad_set_id", "ad_set_name", "impressions", "clicks", "spend",
    "conversions", "conversion_value", "cpc", "currency",
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "MARKETING_ANALYTICS.duckdb"


# ── DuckDB backend ──────────────────────────────────────────────────────────--

# DuckDB DDL for the RAW tables — mirrors snowflake/setup.sql, with DuckDB types.
DUCKDB_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS RAW.{table} (
    date              DATE,
    channel           VARCHAR,
    campaign_id       VARCHAR,
    campaign_name     VARCHAR,
    objective         VARCHAR,
    ad_set_id         VARCHAR,
    ad_set_name       VARCHAR,
    impressions       INTEGER,
    clicks            INTEGER,
    spend             DECIMAL(12,2),
    conversions       INTEGER,
    conversion_value  DECIMAL(12,2),
    cpc               DECIMAL(10,4),
    currency          VARCHAR,
    _loaded_at        TIMESTAMP DEFAULT current_timestamp
)
"""


def load_duckdb(truncate: bool) -> None:
    import duckdb

    db_path = Path(os.environ.get("DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH)))
    print(f"\nConnecting to DuckDB at {db_path}")
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS RAW")

        cols = ", ".join(CSV_COLUMNS)
        for csv_file, table in CSV_TABLE_MAP.items():
            csv_path = DATA_DIR / csv_file
            print(f"Loading {csv_file} -> RAW.{table}")

            con.execute(DUCKDB_TABLE_DDL.format(table=table))
            if truncate:
                print(f"  Truncating RAW.{table}...")
                con.execute(f"DELETE FROM RAW.{table}")

            # read_csv_auto matches columns by header name; we select the known
            # column list explicitly so _loaded_at falls back to its DEFAULT.
            con.execute(
                f"""
                INSERT INTO RAW.{table} ({cols})
                SELECT {cols}
                FROM read_csv_auto('{csv_path.as_posix()}', header = true)
                """
            )
            count = con.execute(f"SELECT count(*) FROM RAW.{table}").fetchone()[0]
            print(f"  OK RAW.{table}: {count:,} rows total\n")

        print("All files loaded successfully.\n")
    finally:
        con.close()


# ── Snowflake backend ─────────────────────────────────────────────────────────

def load_snowflake(truncate: bool) -> None:
    import snowflake.connector

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    print("\nConnecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "MARKETING_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MARKETING_ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.environ.get("SNOWFLAKE_ROLE", "MARKETING_PIPELINE_ROLE"),
    )
    cursor = conn.cursor()
    cols = ", ".join(CSV_COLUMNS)
    try:
        for csv_file, table in CSV_TABLE_MAP.items():
            csv_path = DATA_DIR / csv_file
            stage = f"@%{table}"
            print(f"Loading {csv_file} -> {table}")

            if truncate:
                print(f"  Truncating {table}...")
                cursor.execute(f"TRUNCATE TABLE IF EXISTS {table}")

            cursor.execute(f"REMOVE {stage}")
            cursor.execute(f"PUT 'file://{csv_path.as_posix()}' {stage} AUTO_COMPRESS=TRUE")
            cursor.execute(
                f"""
                COPY INTO {table} ({cols})
                FROM {stage}
                FILE_FORMAT = (
                    TYPE            = 'CSV'
                    FIELD_DELIMITER = ','
                    SKIP_HEADER     = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF         = ('', 'NULL')
                    EMPTY_FIELD_AS_NULL = TRUE
                )
                ON_ERROR = 'ABORT_STATEMENT'
                """
            )
            for row in cursor.fetchall():
                print(f"  OK {table}: {row[3]} rows loaded from {row[0]}")
            cursor.execute(f"REMOVE {stage}")
            print()
        print("All files loaded successfully.\n")
    finally:
        cursor.close()
        conn.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw CSVs into the warehouse")
    parser.add_argument(
        "--target",
        choices=["duckdb", "snowflake"],
        default=os.environ.get("LOAD_TARGET", "duckdb"),
        help="Warehouse backend to load into (default: duckdb)",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target tables before loading (idempotent reruns)",
    )
    args = parser.parse_args()

    for csv_file in CSV_TABLE_MAP:
        path = DATA_DIR / csv_file
        if not path.exists():
            print(f"ERROR: Expected CSV not found: {path}")
            sys.exit(1)

    if args.target == "duckdb":
        load_duckdb(truncate=args.truncate)
    else:
        load_snowflake(truncate=args.truncate)


if __name__ == "__main__":
    main()
