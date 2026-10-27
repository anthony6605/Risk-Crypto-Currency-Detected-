{{
    config(
        materialized='incremental',
        unique_key=['base_asset', 'hour_start'],
        incremental_strategy='delete+insert'
    )
}}

with bars as (

    select * from {{ ref('stg_price_bars') }}

    {% if is_incremental() %}
    -- Rebuild the latest hour already in the table (it may have been partial) and everything after it
    where bar_start >= (select max(hour_start) from {{ this }})
    {% endif %}

)

select
    base_asset,
    date_trunc('hour', bar_start, 'UTC')                    as hour_start,
    (array_agg(open_price order by bar_start))[1]           as open_price,
    max(high_price)                                         as high_price,
    min(low_price)                                          as low_price,
    (array_agg(close_price order by bar_start desc))[1]     as close_price,
    count(*)                                                as minutes_observed,
    -- One poll every 10 minutes gives about 6 bars an hour; 5 or more counts as complete
    count(*) >= 5                                           as is_complete_hour
from bars
group by 1, 2
