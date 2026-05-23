-- Gold: one analyzable row per feedback item, joining the cleaned source to the
-- LLM enrichment. Incremental on the enrichment watermark — only newly enriched
-- rows are processed on each run, so re-running is cheap and the costly LLM step
-- upstream is never repeated for rows already landed here.

{{
    config(
        materialized='incremental',
        unique_key='feedback_id',
        incremental_strategy='delete+insert'
    )
}}

with enriched as (

    select * from {{ ref('stg_feedback_enriched') }}

    {% if is_incremental() %}
    -- only rows enriched after the latest watermark already in this table
    where _enriched_at > (select coalesce(max(_enriched_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}

),

feedback as (

    select * from {{ ref('stg_customer_feedback') }}

)

select
    e.feedback_id,
    f.posted_date,
    f.source_channel,
    f.rating_value,

    e.sentiment,
    e.sentiment_confidence,
    -- numeric score for averaging: +1 positive, -1 negative, 0 otherwise
    case e.sentiment
        when 'positive' then 1
        when 'negative' then -1
        else 0
    end                                                  as sentiment_score,

    e.language,
    e.resolved_campaign_id,
    e.resolution_confidence,
    e.campaign_reference,

    len(from_json(e.themes_json, '["VARCHAR"]'))         as theme_count,

    e.model_version,
    e._enriched_at

from enriched e
left join feedback f using (feedback_id)
