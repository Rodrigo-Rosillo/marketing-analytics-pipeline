# Dashboard

Three-page Power BI report (`marketing_analytics_dashboard.pbix`) plus a fourth
page surfacing the LLM-derived customer sentiment.

## Data source

The marts are served as Parquet files rather than via a live warehouse connection
(the warehouse is now local DuckDB). Regenerate the exports any time after a
`dbt build`:

```bash
python dashboard/export_marts.py        # writes dashboard/exports/*.parquet
```

The exports are git-ignored (they're derived from the marts). To (re)connect Power
BI: **Get Data → Folder** → `dashboard/exports/`, or **Get Data → Parquet** for an
individual file.

| Parquet file | Grain | Use |
|---|---|---|
| `fct_ad_spend.parquet` | date + ad set | Spend / clicks / conversions detail |
| `fct_channel_daily.parquet` | date + channel | Daily channel trends |
| `fct_campaign_summary.parquet` | campaign | Lifetime campaign KPIs |
| `dim_campaigns.parquet` | campaign | Campaign dimension |
| `fct_feedback.parquet` | feedback item | Enriched feedback (sentiment, language, resolved campaign) |
| `fct_feedback_themes.parquet` | feedback × theme | Theme frequency |
| `fct_campaign_performance.parquet` | campaign | **Spend + ROAS joined to sentiment** |

## Suggested "Voice of Customer" page

Built on `fct_campaign_performance`, `fct_feedback`, and `fct_feedback_themes`:

- **Spend vs. net sentiment** scatter — `total_spend` (x) vs `net_sentiment_score`
  (y), bubble size = `feedback_count`, color = `channel`. Surfaces campaigns that
  cost a lot *and* draw negative sentiment (e.g. the low-ROAS TikTok awareness
  campaign).
- **Sentiment mix by campaign** — 100% stacked bar of positive / negative /
  neutral / mixed counts.
- **Top themes** — bar chart of `theme` frequency from `fct_feedback_themes`,
  sliceable by campaign and sentiment.
- **Language split** and **resolution confidence** cards from `fct_feedback`.
- A KPI card noting the share of feedback the model could resolve to a campaign,
  with average `resolution_confidence`.

> The feedback data is synthetic and labeled as such; sentiment, themes, and
> campaign attribution are produced by an LLM (Gemini) — see the project README
> and `AGENTIC.md`.
