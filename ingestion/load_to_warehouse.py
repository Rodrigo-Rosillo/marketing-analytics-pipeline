"""
load_to_warehouse.py
Bulk-loads the raw CSVs from data/raw/ into the Bronze (RAW) schema of either
DuckDB (default, local/CI) or Snowflake (production).

The two engines share one medallion model; only the load mechanics differ:
  - DuckDB:    read_csv_auto into a local file database (no account, no secrets)
  - Snowflake: PUT + COPY INTO via the snowflake connector

Datasets are described once in DATASETS and loaded the same way on both engines.
Bronze keeps everything as-loaded; the customer feedback columns are all VARCHAR
because the source is intentionally messy (dirty dates, mixed-format ratings) and
parsing belongs in the Silver layer, not Bronze.

Usage:
    python ingestion/load_to_warehouse.py                       # DuckDB, append
    python ingestion/load_to_warehouse.py --truncate            # DuckDB, reload
    python ingestion/load_to_warehouse.py --target snowflake --truncate
"""

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# ── Dataset registry ────────────────────────────────────────────────────────--

@dataclass(frozen=True)
class Dataset:
    csv: str                 # source file in data/raw/
    table: str               # target RAW table
    columns: list[str]       # CSV columns (excludes the _loaded_at audit column)
    duckdb_cols_ddl: str     # column definitions for DuckDB CREATE TABLE
    all_varchar: bool = False  # force every CSV column to VARCHAR (messy sources)


_AD_COLUMNS = [
    "date", "channel", "campaign_id", "campaign_name", "objective",
    "ad_set_id", "ad_set_name", "impressions", "clicks", "spend",
    "conversions", "conversion_value", "cpc", "currency",
]

_AD_DDL = """
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
    currency          VARCHAR
"""

DATASETS: list[Dataset] = [
    Dataset("meta_ads_2024.csv",   "META_ADS",   _AD_COLUMNS, _AD_DDL),
    Dataset("google_ads_2024.csv", "GOOGLE_ADS", _AD_COLUMNS, _AD_DDL),
    Dataset("tiktok_ads_2024.csv", "TIKTOK_ADS", _AD_COLUMNS, _AD_DDL),
    # Bronze keeps the messy feedback exactly as-loaded — all VARCHAR.
    Dataset(
        "customer_feedback_2024.csv", "CUSTOMER_FEEDBACK",
        ["feedback_id", "posted_at", "source", "rating",
         "review_text", "author", "true_campaign_id"],
        """
    feedback_id       VARCHAR,
    posted_at         VARCHAR,
    source            VARCHAR,
    rating            VARCHAR,
    review_text       VARCHAR,
    author            VARCHAR,
    true_campaign_id  VARCHAR
""",
        all_varchar=True,
    ),
]

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "raw"
DEFAULT_DUCKDB_PATH = PROJECT_ROOT / "MARKETING_ANALYTICS.duckdb"


# ── DuckDB backend ──────────────────────────────────────────────────────────--

def load_duckdb(datasets: list[Dataset], truncate: bool) -> None:
    import duckdb

    db_path = Path(os.environ.get("DUCKDB_PATH", str(DEFAULT_DUCKDB_PATH)))
    print(f"\nConnecting to DuckDB at {db_path}")
    con = duckdb.connect(str(db_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS RAW")
        for ds in datasets:
            csv_path = DATA_DIR / ds.csv
            cols = ", ".join(ds.columns)
            print(f"Loading {ds.csv} -> RAW.{ds.table}")

            con.execute(
                f"CREATE TABLE IF NOT EXISTS RAW.{ds.table} ("
                f"{ds.duckdb_cols_ddl}, _loaded_at TIMESTAMP DEFAULT current_timestamp)"
            )
            if truncate:
                print(f"  Truncating RAW.{ds.table}...")
                con.execute(f"DELETE FROM RAW.{ds.table}")

            # read_csv_auto matches by header name; we select the known column
            # list explicitly so _loaded_at falls back to its DEFAULT.
            varchar_opt = ", all_varchar = true" if ds.all_varchar else ""
            con.execute(
                f"INSERT INTO RAW.{ds.table} ({cols}) "
                f"SELECT {cols} FROM read_csv_auto('{csv_path.as_posix()}', "
                f"header = true{varchar_opt})"
            )
            count = con.execute(f"SELECT count(*) FROM RAW.{ds.table}").fetchone()[0]
            print(f"  OK RAW.{ds.table}: {count:,} rows total\n")

        print("All files loaded successfully.\n")
    finally:
        con.close()


# ── Snowflake backend ─────────────────────────────────────────────────────────

def load_snowflake(datasets: list[Dataset], truncate: bool) -> None:
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
    try:
        for ds in datasets:
            csv_path = DATA_DIR / ds.csv
            stage = f"@%{ds.table}"
            cols = ", ".join(ds.columns)
            print(f"Loading {ds.csv} -> {ds.table}")

            if truncate:
                print(f"  Truncating {ds.table}...")
                cursor.execute(f"TRUNCATE TABLE IF EXISTS {ds.table}")

            cursor.execute(f"REMOVE {stage}")
            cursor.execute(f"PUT 'file://{csv_path.as_posix()}' {stage} AUTO_COMPRESS=TRUE")
            cursor.execute(
                f"""
                COPY INTO {ds.table} ({cols})
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
                print(f"  OK {ds.table}: {row[3]} rows loaded from {row[0]}")
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

    for ds in DATASETS:
        if not (DATA_DIR / ds.csv).exists():
            print(f"ERROR: Expected CSV not found: {DATA_DIR / ds.csv}")
            sys.exit(1)

    if args.target == "duckdb":
        load_duckdb(DATASETS, truncate=args.truncate)
    else:
        load_snowflake(DATASETS, truncate=args.truncate)


if __name__ == "__main__":
    main()
