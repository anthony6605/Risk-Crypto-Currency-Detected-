select
    upper(base_asset) as base_asset,
    bar_start,
    open  as open_price,
    high  as high_price,
    low   as low_price,
    close as close_price
from {{ source('riskcoin', 'price_bars_1m') }}
