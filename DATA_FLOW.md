# Data Flow

End-to-end data lineage for the marketing analytics pipeline. Sources flow through Bronze (raw), Silver (staging), and Gold (marts) layers in Snowflake before landing in Power BI.

## Pipeline Overview

```mermaid
flowchart TD
    %% ── Sources ──────────────────────────────────────────────
    subgraph SRC["Data Sources"]
        META_API[Meta Ads API]
        GOOGLE_API[Google Ads API]
        TIKTOK_API[TikTok Ads API]
    end

    %% ── Simulation ──────────────────────────────────────────
    SIM["simulate_ad_data.py<br/>generates synthetic CSVs"]

    %% ── Local CSV files ─────────────────────────────────────
    subgraph CSV["data/raw/ (CSV files)"]
        META_CSV["meta_ads_2024.csv<br/>2,928 rows"]
        GOOGLE_CSV["google_ads_2024.csv<br/>2,562 rows"]
        TIKTOK_CSV["tiktok_ads_2024.csv<br/>1,830 rows"]
    end

    %% ── Loader ──────────────────────────────────────────────
    LOAD["load_to_snowflake.py<br/>PUT + COPY INTO"]

    %% ── Bronze ──────────────────────────────────────────────
    subgraph BRONZE["Snowflake — BRONZE (RAW schema)"]
        RAW_META[(RAW.META_ADS)]
        RAW_GOOGLE[(RAW.GOOGLE_ADS)]
        RAW_TIKTOK[(RAW.TIKTOK_ADS)]
    end

    %% ── Silver ──────────────────────────────────────────────
    subgraph SILVER["Snowflake — SILVER (STAGING schema, dbt views)"]
        STG_META[/stg_meta_ads/]
        STG_GOOGLE[/stg_google_ads/]
        STG_TIKTOK[/stg_tiktok_ads/]
    end

    %% ── Gold ────────────────────────────────────────────────
    subgraph GOLD["Snowflake — GOLD (MARTS schema, dbt tables)"]
        FCT_AD[(fct_ad_spend<br/>grain: date + ad_set)]
        FCT_CHAN[(fct_channel_daily<br/>grain: date + channel)]
        FCT_CAMP[(fct_campaign_summary<br/>grain: campaign)]
        DIM_CAMP[(dim_campaigns<br/>grain: campaign)]
    end

    %% ── BI ──────────────────────────────────────────────────
    PBI["Power BI Dashboard<br/>marketing_analytics_dashboard.pbix<br/>3 pages: Executive · Channel · Campaign"]

    %% ── Edges ──────────────────────────────────────────────
    META_API -.real source.-> SIM
    GOOGLE_API -.real source.-> SIM
    TIKTOK_API -.real source.-> SIM

    SIM --> META_CSV
    SIM --> GOOGLE_CSV
    SIM --> TIKTOK_CSV

    META_CSV --> LOAD
    GOOGLE_CSV --> LOAD
    TIKTOK_CSV --> LOAD

    LOAD --> RAW_META
    LOAD --> RAW_GOOGLE
    LOAD --> RAW_TIKTOK

    RAW_META --> STG_META
    RAW_GOOGLE --> STG_GOOGLE
    RAW_TIKTOK --> STG_TIKTOK

    STG_META --> FCT_AD
    STG_GOOGLE --> FCT_AD
    STG_TIKTOK --> FCT_AD

    FCT_AD --> FCT_CHAN
    FCT_AD --> FCT_CAMP
    FCT_AD --> DIM_CAMP

    FCT_AD --> PBI
    FCT_CHAN --> PBI
    FCT_CAMP --> PBI
    DIM_CAMP --> PBI

    %% ── Styling ─────────────────────────────────────────────
    classDef bronze fill:#cd7f32,stroke:#5a3a17,color:#fff
    classDef silver fill:#c0c0c0,stroke:#555,color:#000
    classDef gold fill:#ffd700,stroke:#7a5c00,color:#000
    classDef python fill:#306998,stroke:#1a3a5e,color:#fff
    classDef bi fill:#f2c811,stroke:#8a6f00,color:#000

    class RAW_META,RAW_GOOGLE,RAW_TIKTOK bronze
    class STG_META,STG_GOOGLE,STG_TIKTOK silver
    class FCT_AD,FCT_CHAN,FCT_CAMP,DIM_CAMP gold
    class SIM,LOAD python
    class PBI bi
```

## Layer Responsibilities

| Layer | Tool | What happens | Materialization |
|---|---|---|---|
| Source | Python | `simulate_ad_data.py` writes 3 CSVs to `data/raw/` | CSV files |
| Ingestion | Python + snowflake-connector | `load_to_snowflake.py` runs `PUT` then `COPY INTO`, sets `_loaded_at` audit column | — |
| Bronze | Snowflake | Raw rows mirrored as-is from CSV, no transforms | Tables |
| Silver | dbt | Type casts, filters `impressions = 0`, derives `click_through_rate`, `cost_per_conversion`, `roas`, generates `surrogate_key` from `date + ad_set_id` | Views |
| Gold | dbt | `fct_ad_spend` unions all three staging models; `fct_channel_daily`, `fct_campaign_summary`, `dim_campaigns` aggregate from `fct_ad_spend` | Tables |
| BI | Power BI | Reads Gold tables via Snowflake connector | .pbix file |

## Orchestration Flow (Airflow DAG)

The [marketing_pipeline_dag.py](airflow/dags/marketing_pipeline_dag.py) DAG runs daily at 06:00 UTC.

```mermaid
flowchart LR
    A[simulate_data<br/>PythonOperator] --> B[load_to_snowflake<br/>PythonOperator]
    B --> C["dbt_run<br/>BashOperator<br/>dbt run --select staging marts"]
    C --> D[dbt_test<br/>BashOperator<br/>43 tests]
    D --> E[notify_success<br/>PythonOperator]

    classDef task fill:#017cee,stroke:#003d75,color:#fff
    class A,B,C,D,E task
```

## CI/CD Flow (GitHub Actions)

```mermaid
flowchart LR
    PR[Pull Request] --> CI["ci.yml<br/>dbt compile<br/>dbt test --select staging"]
    CI -->|pass| MERGE[Merge to main]
    CI -->|fail| BLOCK[PR blocked]

    MERGE --> DEPLOY["deploy.yml<br/>dbt run<br/>dbt test<br/>dbt docs generate"]
    DEPLOY --> PAGES[GitHub Pages<br/>dbt docs site]

    classDef pass fill:#2ea44f,stroke:#1a6e30,color:#fff
    classDef fail fill:#cf222e,stroke:#8a0d18,color:#fff
    class MERGE,DEPLOY,PAGES pass
    class BLOCK fail
```

## Data Quality Gates

- **Source-level** — `not_null` tests on RAW columns via `sources.yml`
- **Staging** — `unique` + `not_null` on `surrogate_key`; `not_null` on `date`, `channel`, `campaign_id`, `ad_set_id`
- **Marts** — `dbt_expectations.expect_column_values_to_be_between` on `spend` (0–10,000) and `roas` (0–50) in `fct_ad_spend`
- **CI** — staging tests must pass before any PR can merge to `main`
