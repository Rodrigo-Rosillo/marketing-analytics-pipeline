with unioned as (

    select * from {{ ref('stg_meta_ads') }}
    union all
    select * from {{ ref('stg_google_ads') }}
    union all
    select * from {{ ref('stg_tiktok_ads') }}

)

select
    surrogate_key,
    date,
    channel,
    campaign_id,
    campaign_name,
    objective,
    ad_set_id,
    ad_set_name,
    impressions,
    clicks,
    spend,
    conversions,
    conversion_value,
    click_through_rate,
    cost_per_conversion,
    roas

from unioned
