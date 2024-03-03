with txn_grouped_by_date as(
    select
        date,
        sum(amount)::numeric(38,2) as tot_amount
    from {{ ref("fact_txn") }}
    group by date
)
select
    a.*,
    coalesce(b.event, 'N/A') as event
from txn_grouped_by_date a
left join {{ ref("dim_event") }} b on a.date = b.date
order by tot_amount desc

