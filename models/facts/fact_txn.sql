with txn_ranked as (
    select
        *,
        row_number() over (partition by date order by amount) as rank
    from {{ source("bank", "txn") }}
)

select
    {{ dbt_utils.generate_surrogate_key(['date']) }} as date_key,
    {{ dbt_utils.generate_surrogate_key(['date', 'amount', '"desc"', 'tags', 'priority', 'source', 'planned', 'rank']) }} as txn_key,
    amount,
    date,
    "desc",
    tags,
    priority
    source,
    planned
from txn_ranked

