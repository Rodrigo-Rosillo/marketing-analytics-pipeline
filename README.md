# LLM-Enriched Marketing Analytics Pipeline

Multi-channel ad performance **and LLM-enriched customer feedback** in one
warehouse. Meta / Google / TikTok ad data plus messy, unstructured reviews flow
through a medallion (Bronze → Silver → Gold) model in dbt, with the feedback
enriched by an LLM (Google Gemini) — sentiment, themes, entity extraction, and
free-text-to-campaign resolution — then tested end to end and orchestrated.

Runs on **DuckDB** locally and in CI (free, no secrets), and deploys to
**Snowflake** in production — warehouse-portable by design.

---

## Dashboard

Power BI report built on the Gold layer, served via Parquet exports
([dashboard/README.md](dashboard/README.md)). Slicers for date range, channel, and
campaign filter all pages.

### Executive Summary
![Executive Summary](dashboard/screenshots/page1_executive_summary.png)

### Channel Performance
![Channel Performance](dashboard/screenshots/page2_channel_performance.png)

### Campaign Detail
![Campaign Detail](dashboard/screenshots/page3_campaign_detail.png)

[Download .pbix](dashboard/marketing_analytics_dashboard.pbix)

> **Note:** The dashboard currently covers ad performance only (Executive
> Summary, Channel Performance, Campaign Detail). A Voice-of-Customer page —
> showing sentiment trends, theme breakdown, and campaign-level feedback
> scores on the enriched feedback — is in progress.

---

## Architecture

```
┌──────────────────────────────┐     ┌──────────────────────────────────┐
│  Ad platforms (CSV / API)    │     │  Customer feedback (reviews,      │
│  Meta · Google · TikTok      │     │  social comments) — messy text    │
└──────────────┬───────────────┘     └──────────────┬───────────────────┘
               │ load_to_warehouse.py                │
               ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  BRONZE (RAW)  — raw rows, untouched                                      │
│  RAW.META_ADS · GOOGLE_ADS · TIKTOK_ADS · CUSTOMER_FEEDBACK               │
└──────────────┬──────────────────────────────────┬────────────────────────┘
               │                                   │  enrich_feedback.py (Gemini)
               │                                   ▼
               │                  ┌──────────────────────────────────────────┐
               │                  │  RAW.FEEDBACK_ENRICHED                    │
               │                  │  sentiment · themes · entities · language │
               │                  │  · resolved_campaign_id · confidence      │
               │                  └──────────────────┬───────────────────────┘
               ▼                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  SILVER (STAGING, dbt views) — typed, cleaned, derived metrics            │
│  stg_*_ads · stg_customer_feedback · stg_feedback_enriched                │
└──────────────────────────────────┬────────────────────────────────────────┘
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  GOLD (MARTS, dbt tables)                                                 │
│  fct_ad_spend · fct_channel_daily · fct_campaign_summary · dim_campaigns  │
│  fct_feedback (incremental) · fct_feedback_themes · fct_campaign_performance│
│                          + feedback_enrichment_snapshot (SCD2)            │
└──────────────────────────────────┬────────────────────────────────────────┘
                                    ▼
                          Power BI (via Parquet)
```

**Engines:** DuckDB (default — local + CI, no secrets) · Snowflake (`--target prod`).

**Orchestration:** Airflow DAG — simulate ads + feedback → load → enrich (offline) → `dbt build` → notify.

**CI/CD:** every PR runs the **entire pipeline on DuckDB with no secrets** (generate → load → offline enrich → full `dbt build` + tests). Merges to `main` rebuild and publish dbt docs to GitHub Pages.

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Python | Load CSVs into Bronze (DuckDB `read_csv_auto` / Snowflake `COPY INTO`) |
| LLM enrichment | Google Gemini (`google-genai`) + Pydantic | Structured enrichment of unstructured feedback |
| Warehouse (dev/CI) | DuckDB + dbt-duckdb | Free, file-based, zero-secret CI |
| Warehouse (prod) | Snowflake + dbt-snowflake | Cloud deploy target |
| Transformation | dbt Core + dbt_utils + dbt_expectations | Typed models, snapshots, data-quality tests |
| Orchestration | Apache Airflow | Daily DAG with retries |
| CI/CD | GitHub Actions | Full keyless pipeline on PRs; docs to Pages |
| BI | Power BI | Report on the Gold layer (Parquet) |

---

## LLM Enrichment

[`enrichment/enrich_feedback.py`](enrichment/enrich_feedback.py) turns messy free-text
feedback into structured fields using Gemini with **enforced JSON output** (a Pydantic
schema), not free-form text:

- **sentiment** + confidence (classification)
- **themes** from a controlled taxonomy (multi-label classification / normalization)
- **product / competitor mentions** (entity extraction)
- **language** (detection)
- **resolved_campaign_id** + confidence — maps an oblique reference like *"your summer
  sale ad"* to a canonical campaign (entity resolution)

**Engineered for a constrained free tier and reproducible CI:**

- **Content-hash cache + committed fixture** — every enrichment is written to
  `enrichment/fixtures/feedback_enrichment.jsonl`. `--offline` replays it with **no API
  key**, so CI and the daily DAG are deterministic and cost nothing.
- **Per-batch checkpointing + graceful daily-quota stop** — survives the free tier's
  ~20-requests/day cap and resumes.
- **Validation layer** rejects out-of-vocabulary labels and hallucinated campaign IDs.
- **`model_version` stamped on every row**, feeding an SCD2 snapshot.

**Resolution quality vs. held-out ground-truth labels:** **84.5% exact match,
90% precision, 100% specificity** (never invents a campaign when none is referenced),
0 hallucinated IDs.

---

## Data Model

### Bronze — `RAW`
Raw rows, no transforms. Three ad tables (shared schema) plus `CUSTOMER_FEEDBACK`
(all VARCHAR — the messy source is stored as-is) and `FEEDBACK_ENRICHED` (LLM output).

### Silver — `STAGING` (views)
`stg_*_ads` cast/derive CTR, CPA, ROAS. `stg_customer_feedback` normalizes ~20
inconsistent source labels to a channel, parses 7 date formats, and extracts numeric
ratings. `stg_feedback_enriched` types the LLM output.

### Gold — `MARTS` (tables)

| Model | Grain | Description |
|---|---|---|
| `fct_ad_spend` | date + ad set | Union of all staging ad models |
| `fct_channel_daily` | date + channel | Daily channel aggregates |
| `fct_campaign_summary` | campaign | Lifetime campaign metrics |
| `dim_campaigns` | campaign | Campaign dimension |
| `fct_feedback` | feedback item | **Incremental** — enriched, analyzable feedback |
| `fct_feedback_themes` | feedback × theme | Bridge (unnested themes) |
| `fct_campaign_performance` | campaign | **Spend + ROAS joined to sentiment** |

**Snapshot:** `feedback_enrichment_snapshot` (SCD2 on `model_version`) captures how
enrichment changes when the model or prompt changes.

---

## Data Quality

Tests run on every `dbt build` and block merges if they fail:

- `unique` / `not_null` on surrogate and foreign keys across all layers
- Source-level `not_null` on RAW tables
- `dbt_expectations` range checks on `spend`, `roas`
- **On the non-deterministic LLM output:** `accepted_values` (sentiment, themes,
  channel), `relationships` from `resolved_campaign_id` → `dim_campaigns`, confidence
  bounds [0,1], and a **singular test that fails the build if resolution precision
  drops below 80%** vs. the ground-truth label.

---

## Design Decisions

**DuckDB for dev/CI, Snowflake for prod.** My Snowflake trial expired, so I made DuckDB
the default engine and kept Snowflake as a documented `prod` target. The whole medallion
ports with no SQL changes, and CI now runs the *entire* pipeline for free with no
secrets — a more capable setup than the original Snowflake-only CI, which could only
`compile` and test staging. Developing on a local engine and deploying to a cloud
warehouse is a real, increasingly common pattern.

**LLM enrichment upstream of dbt.** The Gemini call is non-deterministic and
rate-limited, so it lives *before* the transformation layer, writing a structured RAW
table. dbt then treats it like any other source — fully reproducible and free to
re-run. The committed fixture makes the non-deterministic step replayable in CI.

**Fixtures over live API in automation.** The free tier allows ~20 requests/day, so the
daily DAG and CI replay a committed fixture offline; only one manual workflow ever
spends quota. This keeps automation deterministic and zero-cost.

**dbt for transformations.** Built-in testing, column-level docs, and a lineage graph.
The tests made it safe to bolt a whole new source and LLM layer onto the existing
models — any breakage shows up immediately.

**Medallion architecture.** Bronze is never modified after load; Silver is unambiguous
casting/cleaning; business logic lives in Gold where it's visible and testable. The
messy feedback proves the point — all parsing happens in Silver, Bronze stays raw.

---

## How to Run Locally

### Prerequisites
- Python 3.11+
- (Optional) a Google AI Studio API key for *live* enrichment — not needed to run the
  pipeline, which replays the committed fixture.

### Setup

```bash
git clone https://github.com/Rodrigo-Rosillo/marketing-analytics-pipeline.git
cd marketing-analytics-pipeline
pip install -r requirements.txt
```

### Run the pipeline (DuckDB, no secrets)

```bash
# 1. Generate synthetic data
python ingestion/simulate_ad_data.py
python ingestion/simulate_feedback_data.py

# 2. Load Bronze (DuckDB)
python ingestion/load_to_warehouse.py --truncate

# 3. Enrich feedback — offline (replays the committed fixture, no API key)
python enrichment/enrich_feedback.py --offline

# 4. Build + test everything
cd dbt
dbt deps
dbt build --profiles-dir . --target dev      # models, snapshot, all tests
dbt docs generate --profiles-dir . && dbt docs serve

# 5. (Optional) export marts for Power BI
cd ..
python dashboard/export_marts.py
```

### Live enrichment (optional)

```bash
# Requires GOOGLE_AI_STUDIO_API_KEY in your environment or .env
python enrichment/enrich_feedback.py --model gemini-2.5-flash --batch-size 120
```

### Deploy to Snowflake (optional prod target)

```bash
# Run snowflake/setup.sql once, set SNOWFLAKE_* env vars, then:
python ingestion/load_to_warehouse.py --target snowflake --truncate
cd dbt && dbt build --profiles-dir . --target prod
```

---

## Project Structure

```
marketing-analytics-pipeline/
├── AGENTIC.md                       # how this was built; design ownership
├── requirements.txt
├── .github/workflows/
│   ├── ci.yml                       # PR: full keyless DuckDB pipeline + tests
│   ├── deploy.yml                   # main: build + dbt docs to Pages
│   └── live-enrichment.yml          # manual: live Gemini run, refresh fixture
├── airflow/dags/marketing_pipeline_dag.py
├── dashboard/
│   ├── export_marts.py              # Gold marts -> Parquet for Power BI
│   ├── README.md
│   └── marketing_analytics_dashboard.pbix
├── data/raw/                        # generated CSVs (ads + feedback)
├── dbt/
│   ├── models/staging/              # stg ads + stg_customer_feedback + stg_feedback_enriched
│   ├── models/marts/                # ad marts + fct_feedback / themes / campaign_performance
│   ├── snapshots/                   # feedback_enrichment_snapshot
│   └── tests/                       # assert_resolution_precision.sql
├── enrichment/
│   ├── enrich_feedback.py           # Gemini enrichment engine
│   └── fixtures/                    # committed enrichment fixture (keyless CI)
├── ingestion/
│   ├── simulate_ad_data.py
│   ├── simulate_feedback_data.py
│   └── load_to_warehouse.py         # DuckDB (default) or Snowflake
└── snowflake/setup.sql              # prod warehouse + table DDL
```
