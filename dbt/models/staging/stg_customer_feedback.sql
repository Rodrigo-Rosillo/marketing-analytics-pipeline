-- Silver: clean the messy raw feedback. This is where parsing belongs — Bronze
-- kept everything as raw strings. We normalize the inconsistent source labels to
-- a channel, parse the multi-format timestamps, and extract a numeric rating.

with source as (

    select * from {{ source('raw', 'CUSTOMER_FEEDBACK') }}

),

staged as (

    select
        feedback_id,
        review_text,
        nullif(trim(author), '')                 as author,

        source                                   as source_raw,

        -- Normalize ~20 inconsistent platform labels down to a channel.
        case
            when lower(trim(source)) in ('fb', 'facebook', 'meta', 'fb comment', 'ig', 'instagram', 'insta') then 'meta'
            when lower(trim(source)) in ('google', 'google reviews', 'google play', 'play store', 'gmb')       then 'google_ads'
            when lower(trim(source)) in ('tiktok', 'tt', 'tt comment')                                         then 'tiktok'
            else 'other'
        end                                      as source_channel,

        posted_at                                as posted_at_raw,

        -- Parse the dirty, multi-format timestamps; unparseable ones stay null.
        coalesce(
            try_strptime(trim(posted_at), '%Y-%m-%d'),
            try_strptime(trim(posted_at), '%Y-%m-%d %H:%M:%S'),
            try_strptime(trim(posted_at), '%m/%d/%Y'),
            try_strptime(trim(posted_at), '%m/%d/%y'),
            try_strptime(trim(posted_at), '%B %d, %Y'),
            try_strptime(trim(posted_at), '%Y.%m.%d'),
            try_strptime(trim(posted_at), '%-d %b %Y')
        )::date                                  as posted_date,

        rating                                   as rating_raw,

        -- Extract a 1-5 score from "5", "5/5", "4.0", "5 stars", or filled stars.
        case
            when rating is null                  then null
            when rating like '%★%'               then length(rating) - length(replace(rating, '★', ''))
            else try_cast(regexp_extract(rating, '\d+') as integer)
        end                                      as rating_value,

        -- Synthetic ground-truth label (simulation artifact, used only to score
        -- resolution accuracy in tests — never an input to the enrichment).
        nullif(true_campaign_id, '')             as true_campaign_id,

        _loaded_at

    from source

)

select * from staged
