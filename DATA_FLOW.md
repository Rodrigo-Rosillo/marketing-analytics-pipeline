# Data Flow

End-to-end lineage. Two sources — structured ad data and unstructured customer
feedback — flow through Bronze → Silver → Gold. Feedback is LLM-enriched between
Bronze and Silver. The warehouse is DuckDB locally and in CI, Snowflake in prod.

## Pipeline Overview

```mermaid
flowchart TD
    %% ── Sources ──────────────────────────────────────────────
    subgraph SRC["Data Sources"]
        ADS[Meta / Google / TikTok<br/>ad platforms]
        FBSRC[Customer feedback<br/>reviews · social comments]
    end

    SIM_ADS["simulate_ad_data.py"]
    SIM_FB["simulate_feedback_data.py<br/>messy free text"]

    subgraph CSV["data/raw/ (CSV)"]
        ADS_CSV["*_ads_2024.csv"]
        FB_CSV["customer_feedback_2024.csv"]
    end

    LOAD["load_to_warehouse.py<br/>DuckDB (default) / Snowflake"]
    ENRICH["enrich_feedback.py<br/>Gemini · structured output<br/>cache + offline fixture"]

    %% ── Bronze ──────────────────────────────────────────────
    subgraph BRONZE["BRONZE — RAW schema"]
        RAW_ADS[(RAW.META/GOOGLE/TIKTOK_ADS)]
        RAW_FB[(RAW.CUSTOMER_FEEDBACK<br/>all VARCHAR, raw)]
        RAW_ENR[(RAW.FEEDBACK_ENRICHED<br/>sentiment · themes · entities<br/>resolved_campaign_id)]
    end

    %% ── Silver ──────────────────────────────────────────────
    subgraph SILVER["SILVER — STAGING (dbt views)"]
        STG_ADS[/stg_meta/google/tiktok_ads/]
        STG_FB[/stg_customer_feedback/]
        STG_ENR[/stg_feedback_enriched/]
    end

    %% ── Gold ────────────────────────────────────────────────
    subgraph GOLD["GOLD — MARTS (dbt tables)"]
        FCT_AD[(fct_ad_spend)]
        FCT_CHAN[(fct_channel_daily)]
        FCT_CAMP[(fct_campaign_summary)]
        DIM_CAMP[(dim_campaigns)]
        FCT_FB[(fct_feedback<br/>incremental)]
        FCT_THEME[(fct_feedback_themes)]
        FCT_PERF[(fct_campaign_performance<br/>spend + sentiment)]
    end

    SNAP[["feedback_enrichment_snapshot<br/>SCD2 on model_version"]]
    PBI["Power BI<br/>(via Parquet exports)"]

    %% ── Edges ──────────────────────────────────────────────
    ADS --> SIM_ADS --> ADS_CSV --> LOAD --> RAW_ADS
    FBSRC --> SIM_FB --> FB_CSV --> LOAD --> RAW_FB
    RAW_FB --> ENRICH --> RAW_ENR
    RAW_ADS --> STG_ADS
    RAW_FB --> STG_FB
    RAW_ENR --> STG_ENR
    RAW_ENR --> SNAP

    STG_ADS --> FCT_AD
    FCT_AD --> FCT_CHAN
    FCT_AD --> FCT_CAMP
    FCT_AD --> DIM_CAMP
    STG_ENR --> FCT_FB
    STG_FB --> FCT_FB
    STG_ENR --> FCT_THEME
    FCT_FB --> FCT_PERF
    FCT_CAMP --> FCT_PERF

    FCT_AD --> PBI
    FCT_CHAN --> PBI
    FCT_PERF --> PBI
    FCT_THEME --> PBI

    %% ── Styling ─────────────────────────────────────────────
    classDef bronze fill:#cd7f32,stroke:#5a3a17,color:#fff
    classDef silver fill:#c0c0c0,stroke:#555,color:#000
    classDef gold fill:#ffd700,stroke:#7a5c00,color:#000
    classDef python fill:#306998,stroke:#1a3a5e,color:#fff
    classDef llm fill:#8e44ad,stroke:#4a235a,color:#fff
    classDef bi fill:#f2c811,stroke:#8a6f00,color:#000

    class RAW_ADS,RAW_FB,RAW_ENR bronze
    class STG_ADS,STG_FB,STG_ENR silver
    class FCT_AD,FCT_CHAN,FCT_CAMP,DIM_CAMP,FCT_FB,FCT_THEME,FCT_PERF,SNAP gold
    class SIM_ADS,SIM_FB,LOAD python
    class ENRICH llm
    class PBI bi
```

## Layer Responsibilities

| Layer | Tool | What happens | Materialization |
|---|---|---|---|
| Source | Python | Simulate ad CSVs + messy feedback CSV | CSV files |
| Ingestion | Python | `load_to_warehouse.py` loads Bronze (DuckDB or Snowflake) | Tables |
| Enrichment | Python + Gemini | `enrich_feedback.py` produces structured fields; cached to a fixture | RAW.FEEDBACK_ENRICHED |
| Bronze | warehouse | Raw rows as-loaded; feedback all-VARCHAR | Tables |
| Silver | dbt | Cast/derive ad metrics; normalize feedback channel/date/rating; type enrichment | Views |
| Gold | dbt | Ad marts; `fct_feedback` (incremental); themes bridge; `fct_campaign_performance` joins spend to sentiment | Tables |
| Snapshot | dbt | SCD2 history of enrichment, keyed on `model_version` | Table |
| BI | Power BI | Reads Gold via Parquet exports | .pbix |

## Orchestration (Airflow DAG)

```mermaid
flowchart LR
    A[simulate_ads] --> C[load_to_warehouse]
    B[simulate_feedback] --> C
    C --> D["enrich_feedback<br/>--offline (fixture)"]
    D --> E["dbt_build<br/>models + snapshot + tests"]
    E --> F[notify_success]

    classDef task fill:#017cee,stroke:#003d75,color:#fff
    class A,B,C,D,E,F task
```

## CI/CD (GitHub Actions)

```mermaid
flowchart LR
    PR[Pull Request] --> CI["ci.yml — full pipeline on DuckDB<br/>generate → load → enrich (offline) → dbt build"]
    CI -->|pass| MERGE[Merge to main]
    CI -->|fail| BLOCK[PR blocked]

    MERGE --> DEPLOY["deploy.yml<br/>build on DuckDB + dbt docs generate"]
    DEPLOY --> PAGES[GitHub Pages]

    DISP["live-enrichment.yml<br/>(manual)"] -->|Gemini API| FIX[refresh fixture artifact]

    classDef pass fill:#2ea44f,stroke:#1a6e30,color:#fff
    classDef fail fill:#cf222e,stroke:#8a0d18,color:#fff
    classDef manual fill:#8e44ad,stroke:#4a235a,color:#fff
    class MERGE,DEPLOY,PAGES pass
    class BLOCK fail
    class DISP,FIX manual
```

## Data Quality Gates

- **Source-level** — `not_null` on RAW columns; `unique` feedback_id
- **Staging** — surrogate-key `unique`/`not_null`; `accepted_values` on normalized channel
- **Enrichment (LLM output)** — `accepted_values` (sentiment, themes), confidence in
  [0,1], `relationships` resolved_campaign_id → dim_campaigns, and a singular test
  failing the build if resolution precision < 80% vs. ground truth
- **CI** — the full pipeline + all tests must pass on DuckDB before any PR merges
