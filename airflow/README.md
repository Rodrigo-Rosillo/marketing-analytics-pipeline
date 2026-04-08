# Airflow Orchestration

## DAG: `marketing_analytics_pipeline`

Runs daily at **06:00 UTC** with the following tasks:

```
simulate_data -> load_to_snowflake -> dbt_run -> dbt_test -> notify_success
```

| Task | Type | Description |
|---|---|---|
| `simulate_data` | PythonOperator | Generates synthetic CSV data for Meta, Google, and TikTok ads |
| `load_to_snowflake` | PythonOperator | Loads CSVs into Snowflake RAW schema (truncate + reload) |
| `dbt_run` | BashOperator | Runs dbt models for staging and marts layers |
| `dbt_test` | BashOperator | Runs the full dbt test suite |
| `notify_success` | PythonOperator | Logs a success summary with run timestamp |

## Running Locally with Docker

### 1. Start Airflow

From the project root:

```bash
# Set the Airflow home to the airflow/ directory
export AIRFLOW_HOME=$(pwd)/airflow

# Using the official Docker Compose
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.10.0/docker-compose.yaml'
mv docker-compose.yaml airflow/docker-compose.yaml
```

Add a volume mount for the project root in `docker-compose.yaml` under `x-airflow-common > volumes`:

```yaml
volumes:
  - ./dags:/opt/airflow/dags
  - ../ingestion:/opt/airflow/ingestion
  - ../data:/opt/airflow/data
  - ../dbt:/opt/airflow/dbt
```

Then start:

```bash
cd airflow
docker compose up airflow-init
docker compose up -d
```

The Airflow UI is available at `http://localhost:8080` (default user: `airflow` / `airflow`).

### 2. Install Python Dependencies in Airflow

Add these to a `requirements.txt` or build a custom Docker image:

```
snowflake-connector-python
python-dotenv
dbt-snowflake
```

In `docker-compose.yaml`, set:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "snowflake-connector-python python-dotenv dbt-snowflake"
```

### 3. Set Environment Variables

Add Snowflake credentials as environment variables in `docker-compose.yaml` under `x-airflow-common > environment`:

```yaml
environment:
  SNOWFLAKE_ACCOUNT: your_account_identifier
  SNOWFLAKE_USER: MARKETING_PIPELINE_USER
  SNOWFLAKE_PASSWORD: your_password_here
  SNOWFLAKE_WAREHOUSE: MARKETING_WH
  SNOWFLAKE_DATABASE: MARKETING_ANALYTICS
  SNOWFLAKE_SCHEMA: RAW
  SNOWFLAKE_ROLE: MARKETING_PIPELINE_ROLE
```

Alternatively, use Airflow Variables or Connections:

**Option A — Airflow Variables** (UI: Admin > Variables):

| Key | Value |
|---|---|
| `SNOWFLAKE_ACCOUNT` | `your_account_identifier` |
| `SNOWFLAKE_USER` | `MARKETING_PIPELINE_USER` |
| `SNOWFLAKE_PASSWORD` | `your_password` |

**Option B — Airflow Connection** (UI: Admin > Connections):

| Field | Value |
|---|---|
| Conn Id | `snowflake_default` |
| Conn Type | Snowflake |
| Host | `your_account_identifier` |
| Login | `MARKETING_PIPELINE_USER` |
| Password | `your_password` |
| Schema | `RAW` |
| Extra | `{"warehouse": "MARKETING_WH", "database": "MARKETING_ANALYTICS", "role": "MARKETING_PIPELINE_ROLE"}` |

### 4. Trigger the DAG

```bash
# Via CLI
docker compose exec airflow-worker airflow dags trigger marketing_analytics_pipeline

# Or enable the DAG toggle in the Airflow UI
```
