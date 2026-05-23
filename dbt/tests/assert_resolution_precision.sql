-- LLM quality gate: of the feedback the model resolved to a campaign, at least
-- 80% must match the held-out ground-truth label. Returns rows (i.e. fails) when
-- precision drops below the threshold — catching prompt/model regressions before
-- they reach the marts. This is a data-quality test on non-deterministic output.

with scored as (

    select
        f.resolved_campaign_id,
        c.true_campaign_id
    from {{ ref('fct_feedback') }} f
    join {{ ref('stg_customer_feedback') }} c using (feedback_id)
    where f.resolved_campaign_id is not null

),

precision_calc as (

    select
        sum(case when resolved_campaign_id = true_campaign_id then 1 else 0 end) * 1.0
            / nullif(count(*), 0) as precision
    from scored

)

select precision
from precision_calc
where precision < 0.80
