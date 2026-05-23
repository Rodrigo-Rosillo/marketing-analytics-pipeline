# Airflow Orchestration

## DAG: `marketing_analytics_pipeline`

Runs daily at **06:00 UTC**:

```
simulate_ads ─┐
              ├─> load_to_warehouse ─> enrich_feedback ─> dbt_build ─> notify_success
simulate_feedback ─┘
```

| Task | Type | Description |
|---|---|---|
| `simulate_ads` | PythonOperator | Generates synthetic Meta / Google / TikTok ad CSVs |
| `simulate_feedback` | PythonOperator | Generates the messy customer-feedback CSV |
| `load_to_warehouse` | PythonOperator | Loads all CSVs into the Bronze (RAW) schema — DuckDB by default (`--truncate` for idempotent reloads) |
| `enrich_feedback` | PythonOperator | LLM enrichment in **`--offline`** mode: replays the committed fixture, so the DAG is deterministic, key-free, and never spends API quota |
| `dbt_build` | BashOperator | `dbt build` — runs every model, the snapshot, and all data-quality tests |
| `notify_success` | PythonOperator | Logs a success summary |

### Why enrichment runs offline here

The Gemini free tier is tightly capped, and the enrichment output is committed as a
fixture. The daily DAG replays that fixture (`enrich_feedback.py --offline`) so runs
are reproducible and cost nothing. Refreshing the fixture against the live API is a
separate, occasional job — see [`.github/workflows/live-enrichment.yml`](../.github/workflows/live-enrichment.yml).

## Running Locally with Docker

From the project root:

```bash
export AIRFLOW_HOME=$(pwd)/airflow
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.0/docker-compose.yaml'
mv docker-compose.yaml airflow/docker-compose.yaml
```

Mount the project into the Airflow containers — add under `x-airflow-common > volumes`:

```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ../ingestion:/opt/airflow/ingestion
  - ../enrichment:/opt/airflow/enrichment
  - ../data:/opt/airflow/data
  - ../dbt:/opt/airflow/dbt
```

Install the pipeline dependencies in the containers — set in `docker-compose.yaml`:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "dbt-duckdb duckdb python-dotenv pydantic"
```

No warehouse credentials are needed for the default (DuckDB) target. To target
Snowflake in production instead, set `DBT_TARGET=prod` plus the `SNOWFLAKE_*` env
vars and add `dbt-snowflake` / `snowflake-connector-python` to the requirements.

Then start Airflow:

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

The UI is at `http://localhost:8080` (default `airflow` / `airflow`). Trigger with:

```bash
docker compose exec airflow-worker airflow dags trigger marketing_analytics_pipeline
```
