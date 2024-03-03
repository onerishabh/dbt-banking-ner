select 
    planned, 
    sum(amount)
from {{ ref("fact_txn") }} 
group by planned

