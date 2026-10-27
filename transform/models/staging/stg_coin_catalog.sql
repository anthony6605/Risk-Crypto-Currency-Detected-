select
    lower(symbol) as symbol,
    coin_id,
    name,
    market_cap,
    market_cap_rank,
    updated_at
from {{ source('riskcoin', 'coin_catalog') }}
