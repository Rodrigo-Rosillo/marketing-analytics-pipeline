with ad_spend as (

    select * from {{ ref('fct_ad_spend') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['channel', 'campaign_id']) }} as surrogate_key,

    channel,
    campaign_id,
    campaign_name,
    objective,

    count(distinct date)                                        as total_days_active,
    sum(impressions)                                            as total_impressions,
    sum(clicks)                                                 as total_clicks,
    sum(spend)                                                  as total_spend,
    sum(conversions)                                            as total_conversions,
    sum(conversion_value)                                       as total_conversion_value,

    round(sum(clicks) / nullif(sum(impressions), 0), 6)         as avg_ctr,
    round(sum(spend) / nullif(sum(conversions), 0), 4)          as avg_cpa,
    round(sum(conversion_value) / nullif(sum(spend), 0), 4)     as avg_roas

from ad_spend
group by channel, campaign_id, campaign_name, objective
