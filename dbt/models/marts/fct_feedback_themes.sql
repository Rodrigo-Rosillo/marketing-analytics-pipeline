-- Gold bridge: one row per (feedback, theme). Unnests the LLM theme array so
-- themes can be counted and joined to campaigns/sentiment.
--
-- NOTE: from_json/unnest is DuckDB syntax (dev + CI engine). On Snowflake the
-- equivalent is LATERAL FLATTEN(input => PARSE_JSON(themes_json)); this is the
-- one model with engine-specific JSON handling.

with enriched as (

    select feedback_id, themes_json
    from {{ ref('stg_feedback_enriched') }}

),

exploded as (

    select
        feedback_id,
        unnest(from_json(themes_json, '["VARCHAR"]')) as theme
    from enriched

)

select
    {{ dbt_utils.generate_surrogate_key(['feedback_id', 'theme']) }} as surrogate_key,
    feedback_id,
    theme
from exploded
