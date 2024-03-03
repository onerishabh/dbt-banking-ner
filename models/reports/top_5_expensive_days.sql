with grouped_days as (
    select
        date,
        sum(amount)::numeric(38,2) as tot_amt
    from {{ ref("fact_txn") }}
    group by date
    order by tot_amt desc
    limit 5
),

days_ranked as (
    select
        *,
        row_number() over(order by tot_amt desc) as rank
    from grouped_days
),

-- select * from days_ranked;
txns as (
    select
        days_ranked.date,
        txn.amount,
        txn.tags,
        txn."desc",
        days_ranked.rank,
        days_ranked.tot_amt,
        coalesce(dim_event.event, 'N/A') as event
    from {{ ref("fact_txn") }} txn
    inner join days_ranked on txn.date = days_ranked.date
    left join {{ ref("dim_event") }} dim_event on txn.date = dim_event.date
    order by rank
)

select * from txns

