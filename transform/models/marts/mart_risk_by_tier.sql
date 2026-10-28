select
    dim_coin.market_cap_tier,
    risk.risk_level,
    count(*)                         as coins,
    round(avg(risk.risk_score)::numeric, 1) as avg_risk_score,
    round(avg(risk.vol_24h)::numeric, 3)    as avg_vol_24h
from {{ ref('stg_live_risk') }} as risk
join {{ ref('dim_coin') }} as dim_coin
    using (base_asset)
group by 1, 2
