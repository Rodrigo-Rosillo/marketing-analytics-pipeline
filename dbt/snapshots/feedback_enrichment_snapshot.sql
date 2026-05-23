-- SCD2 snapshot of the LLM enrichment. Because non-deterministic model output
-- changes over time — a re-run, a prompt tweak, or a model upgrade can reclassify
-- a review — we snapshot it. check_cols includes model_version, so swapping the
-- Gemini model produces new history rows and you can see exactly what was
-- reclassified and when (dbt_valid_from / dbt_valid_to).

{% snapshot feedback_enrichment_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='feedback_id',
        strategy='check',
        check_cols=['sentiment', 'resolved_campaign_id', 'themes', 'model_version']
    )
}}

select
    feedback_id,
    sentiment,
    resolved_campaign_id,
    themes,
    model_version
from {{ source('raw', 'FEEDBACK_ENRICHED') }}

{% endsnapshot %}
