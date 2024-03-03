with txns as (
    select
        date,
        amount,
        split(tags, ',') as tags
    from {{ ref("fact_txn") }}
),

tags_flattened as (
    select
        date,
        amount,
        trim(unnest(tags)) as tag
    from txns
)

select
    tag,
    sum(amount)::numeric(38,2) as tot_amount
from tags_flattened
group by tag
order by tot_amount desc

