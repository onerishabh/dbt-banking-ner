select
    priority,
    sum(amount)::numeric(38,2) as tot_amount
from {{ ref("fact_txn") }}
group by priority
order by tot_amount desc

