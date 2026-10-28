with assets as (

    select distinct base_asset from {{ ref('fct_price_hourly') }}

),

catalog as (

    select * from {{ ref('stg_coin_catalog') }}

)

select
    assets.base_asset,
    catalog.coin_id,
    coalesce(catalog.name, assets.base_asset) as coin_name,
    catalog.market_cap,
    catalog.market_cap_rank,
    case
        when catalog.market_cap_rank <= 10  then 'mega'
        when catalog.market_cap_rank <= 100 then 'large'
        when catalog.market_cap_rank <= 500 then 'mid'
        when catalog.market_cap_rank is not null then 'small'
        else 'unranked'
    end as market_cap_tier,
    catalog.coin_id is not null as is_cataloged
from assets
left join catalog
    on catalog.symbol = lower(assets.base_asset)
