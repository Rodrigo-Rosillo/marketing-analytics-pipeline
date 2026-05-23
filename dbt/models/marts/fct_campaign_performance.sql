-- Gold: the headline mart. Joins paid-media performance (spend / ROAS /
-- conversions) to the voice-of-customer signal the LLM extracted, per campaign.
-- This is what the medallion + enrichment buys you: "which campaigns we spend on
-- vs. how customers actually feel about them."

with spend as (

    select * from {{ ref('fct_campaign_summary') }}

),

feedback as (

    select
        resolved_campaign_id                                     as campaign_id,
        count(*)                                                 as feedback_count,
        sum(case when sentiment = 'positive' then 1 else 0 end)  as positive_count,
        sum(case when sentiment = 'negative' then 1 else 0 end)  as negative_count,
        sum(case when sentiment = 'neutral'  then 1 else 0 end)  as neutral_count,
        sum(case when sentiment = 'mixed'    then 1 else 0 end)  as mixed_count,
        round(avg(sentiment_score), 4)                           as net_sentiment_score,
        round(avg(resolution_confidence), 4)                     as avg_resolution_confidence
    from {{ ref('fct_feedback') }}
    where resolved_campaign_id is not null
    group by resolved_campaign_id

)

select
    s.campaign_id,
    s.channel,
    s.campaign_name,
    s.objective,

    -- paid media
    s.total_spend,
    s.total_conversions,
    s.total_conversion_value,
    s.avg_roas,

    -- voice of customer
    coalesce(f.feedback_count, 0)       as feedback_count,
    coalesce(f.positive_count, 0)       as positive_count,
    coalesce(f.negative_count, 0)       as negative_count,
    coalesce(f.neutral_count, 0)        as neutral_count,
    coalesce(f.mixed_count, 0)          as mixed_count,
    f.net_sentiment_score,
    f.avg_resolution_confidence

from spend s
left join feedback f on s.campaign_id = f.campaign_id
