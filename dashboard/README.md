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

## Voice of Customer page

Built on `fct_feedback` and `fct_feedback_themes`:

- **KPI cards** — total feedback, positive %, negative %, average sentiment
  confidence, and average sentiment score (net polarity on a −1…+1 scale).
- **Feedback volume by sentiment** — bar chart of positive / negative / neutral /
  mixed counts.
- **Sentiment trend by month** — line chart of feedback volume per sentiment over
  the year.
- **Sentiment by theme** — 100% stacked bar of `theme` frequency from
  `fct_feedback_themes`, split by sentiment (uses a bidirectional cross-filter so
  the bridge table filters the feedback measure).
- **Sentiment by campaign reference** — stacked bar of the LLM-resolved free-text
  mentions (e.g. *"the dynamic ad on tiktok"*) split by sentiment.

Slicers: campaign reference (search), sentiment, channel, campaign, and date range.

> The feedback data is synthetic and labeled as such; sentiment, themes, and
> campaign attribution are produced by an LLM (Gemini) — see the project README
> and `AGENTIC.md`.
