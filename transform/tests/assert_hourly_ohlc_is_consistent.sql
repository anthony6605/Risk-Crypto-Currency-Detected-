-- Fails if any hourly bar has impossible prices
select *
from {{ ref('fct_price_hourly') }}
where low_price <= 0
   or low_price > high_price
   or open_price  not between low_price and high_price
   or close_price not between low_price and high_price
