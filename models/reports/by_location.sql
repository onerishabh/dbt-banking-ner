with txn as (
    select
        txn_key,
        amount
    from {{ ref("fact_txn") }}
),

ner_tags as (
    select
        *
    from {{ source("ml", "dim_ner") }}
),

txn_tags_joined as (
    select
        txn.amount,
        ner_tags.loc as location
    from txn
    inner join ner_tags on txn.txn_key = ner_tags.txn_key
)

select
    location,
    sum(amount)::numeric(38,2) as tot_amount
from txn_tags_joined
group by 1
order by tot_amount desc

