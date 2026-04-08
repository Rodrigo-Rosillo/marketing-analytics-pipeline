with ad_spend as (

    select * from {{ ref('fct_ad_spend') }}

)

select distinct
    {{ dbt_utils.generate_surrogate_key(['channel', 'campaign_id']) }} as surrogate_key,
    channel,
    campaign_id,
    campaign_name,
    objective

from ad_spend
