with hourly as (

    select
        *,
        -- Only returns between consecutive hours count. After a gap (the stream was
        -- paused) one move covers several hours and would overstate hourly volatility.
        case
            when lag(hour_start) over by_coin = hour_start - interval '1 hour'
            then ln(close_price / lag(close_price) over by_coin)
        end as log_return
    from {{ ref('fct_price_hourly') }}
    window by_coin as (partition by base_asset order by hour_start)

),

daily as (

    select
        base_asset,
        (hour_start at time zone 'UTC')::date                   as price_date,
        (array_agg(open_price order by hour_start))[1]          as open_price,
        max(high_price)                                         as high_price,
        min(low_price)                                          as low_price,
        (array_agg(close_price order by hour_start desc))[1]    as close_price,
        count(*)                                                as hours_observed,
        stddev_samp(log_return) * sqrt(24 * 365)                as realized_vol_annualized
    from hourly
    group by 1, 2

)

select
    *,
    close_price / open_price - 1 as daily_return,
    hours_observed = 24          as is_complete_day
from daily
