-- Silver: type the LLM enrichment output. JSON arrays stay as strings here and
-- are unnested downstream (fct_feedback_themes). Empty resolved_campaign_id is
-- coerced to NULL so the relationship test to dim_campaigns ignores it.

with source as (

    select * from {{ source('raw', 'FEEDBACK_ENRICHED') }}

),

staged as (

    select
        feedback_id,
        lower(trim(sentiment))                   as sentiment,
        sentiment_confidence,

        themes                                   as themes_json,
        product_mentions                         as product_mentions_json,
        competitor_mentions                      as competitor_mentions_json,

        lower(trim(language))                    as language,
        nullif(trim(campaign_reference), '')     as campaign_reference,
        nullif(trim(resolved_campaign_id), '')   as resolved_campaign_id,
        resolution_confidence,

        model_version,
        content_hash,
        _enriched_at

    from source

)

select * from staged
