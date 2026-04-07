"""
load_to_snowflake.py
Bulk-loads CSV files from data/raw/ into Snowflake RAW schema tables
using PUT + COPY INTO for efficient ingestion.

Usage:
    python ingestion/load_to_snowflake.py               # append to existing data
    python ingestion/load_to_snowflake.py --truncate     # clear tables first
"""

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()


# ── Configuration ─────────────────────────────────────────────────────────────

# Maps each CSV filename to its target RAW table
CSV_TABLE_MAP: dict[str, str] = {
    "meta_ads_2024.csv":    "META_ADS",
    "google_ads_2024.csv":  "GOOGLE_ADS",
    "tiktok_ads_2024.csv":  "TIKTOK_ADS",
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_connection() -> snowflake.connector.SnowflakeConnection:
    """Create a Snowflake connection from environment variables."""
    required_vars = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
    ]
    missing = [v for v in required_vars if not os.environ.get(v)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "MARKETING_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "MARKETING_ANALYTICS"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        role=os.environ.get("SNOWFLAKE_ROLE", "MARKETING_PIPELINE_ROLE"),
    )


def load_csv(
    cursor: snowflake.connector.cursor.SnowflakeCursor,
    csv_path: Path,
    table_name: str,
    truncate: bool = False,
) -> None:
    """Load a single CSV into a Snowflake table via PUT + COPY INTO."""
    stage_name = f"@%{table_name}"

    if truncate:
        print(f"  Truncating {table_name}...")
        cursor.execute(f"TRUNCATE TABLE IF EXISTS {table_name}")

    # Remove any previously staged files for this table
    cursor.execute(f"REMOVE {stage_name}")

    # PUT local file into the table stage
    put_sql = f"PUT 'file://{csv_path.as_posix()}' {stage_name} AUTO_COMPRESS=TRUE"
    print(f"  PUT  {csv_path.name} -> {stage_name}")
    cursor.execute(put_sql)

    # COPY staged file into the table (exclude _loaded_at — it has a DEFAULT)
    columns = (
        "date, channel, campaign_id, campaign_name, objective, "
        "ad_set_id, ad_set_name, impressions, clicks, spend, "
        "conversions, conversion_value, cpc, currency"
    )
    copy_sql = f"""
        COPY INTO {table_name} ({columns})
        FROM {stage_name}
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
    print(f"  COPY INTO {table_name}...")
    cursor.execute(copy_sql)

    # Report loaded row count
    result = cursor.fetchall()
    for row in result:
        # COPY INTO returns: file, status, rows_parsed, rows_loaded, ...
        print(f"  OK {table_name}: {row[3]} rows loaded from {row[0]}")

    # Clean up stage
    cursor.execute(f"REMOVE {stage_name}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Load raw CSVs into Snowflake")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target tables before loading (idempotent reruns)",
    )
    args = parser.parse_args()

    # Validate that all expected CSVs exist
    for csv_file in CSV_TABLE_MAP:
        path = DATA_DIR / csv_file
        if not path.exists():
            print(f"ERROR: Expected CSV not found: {path}")
            sys.exit(1)

    print("\nConnecting to Snowflake...")
    conn = get_connection()
    cursor = conn.cursor()

    try:
        print(f"Using warehouse: {cursor.execute('SELECT CURRENT_WAREHOUSE()').fetchone()[0]}")
        print(f"Using database:  {cursor.execute('SELECT CURRENT_DATABASE()').fetchone()[0]}")
        print(f"Using schema:    {cursor.execute('SELECT CURRENT_SCHEMA()').fetchone()[0]}\n")

        for csv_file, table_name in CSV_TABLE_MAP.items():
            csv_path = DATA_DIR / csv_file
            print(f"Loading {csv_file} -> {table_name}")
            load_csv(cursor, csv_path, table_name, truncate=args.truncate)
            print()

        print("All files loaded successfully.\n")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()
