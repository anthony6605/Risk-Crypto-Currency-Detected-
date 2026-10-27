select
    upper(base_asset) as base_asset,
    price,
    return_24h,
    vol_1h,
    vol_24h,
    max_drawdown_24h,
    risk_score,
    risk_level,
    computed_at
from {{ source('riskcoin', 'live_risk') }}
