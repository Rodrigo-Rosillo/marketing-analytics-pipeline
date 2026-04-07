with source as (

    select * from {{ source('raw', 'GOOGLE_ADS') }}

),

staged as (

    select
        {{ dbt_utils.generate_surrogate_key(['date', 'ad_set_id']) }} as surrogate_key,

        date::date                      as date,
        channel::varchar                as channel,
        campaign_id::varchar            as campaign_id,
        campaign_name::varchar          as campaign_name,
        objective::varchar              as objective,
        ad_set_id::varchar              as ad_set_id,
        ad_set_name::varchar            as ad_set_name,
        impressions::int                as impressions,
        clicks::int                     as clicks,
        spend::float                    as spend,
        conversions::int                as conversions,
        conversion_value::float         as conversion_value,
        cpc::float                      as cpc,
        currency::varchar               as currency,

        -- derived metrics
        round(clicks / nullif(impressions, 0), 6)       as click_through_rate,
        round(spend / nullif(conversions, 0), 4)         as cost_per_conversion,
        round(conversion_value / nullif(spend, 0), 4)    as roas,

        _loaded_at

    from source
    where impressions > 0

)

select * from staged
