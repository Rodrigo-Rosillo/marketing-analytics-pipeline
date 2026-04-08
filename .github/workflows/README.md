# CI/CD Workflows

## Workflows

### `ci.yml` — Pull Request Checks

**Trigger:** Every pull request targeting `main`.

| Step | Purpose |
|---|---|
| `dbt compile` | Catches SQL syntax errors and ref/source issues without running queries |
| `dbt test --select staging` | Runs data quality tests on the staging layer against Snowflake |

### `deploy.yml` — Production Deploy + Docs

**Trigger:** Every push to `main` (i.e., merged PRs).

| Step | Purpose |
|---|---|
| `dbt run --select staging marts` | Rebuilds all staging views and mart tables in Snowflake |
| `dbt test` | Runs the full test suite (staging + marts + sources) |
| `dbt docs generate` | Generates the dbt documentation site |
| Deploy to GitHub Pages | Publishes the docs site at `https://<user>.github.io/<repo>/` |

## Required Secrets

Add these in **Settings → Secrets and variables → Actions → Repository secrets**:

| Secret | Example | Description |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | `xy12345.us-east-1` | Snowflake account identifier |
| `SNOWFLAKE_USER` | `MARKETING_PIPELINE_USER` | Service account username |
| `SNOWFLAKE_PASSWORD` | *(your password)* | Service account password |

## GitHub Pages Setup

To enable the docs deployment:

1. Go to **Settings → Pages**
2. Under **Source**, select **GitHub Actions**
