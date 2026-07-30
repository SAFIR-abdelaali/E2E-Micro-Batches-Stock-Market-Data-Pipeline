select 
v:c::float as current_price,
v:d::float as change_amount,
v:dp::float as change_percent,
v:h::float as day_high,
v:l::float as day_low,
v:o::float as day_open,
v:pc::float as prev_close,
v:t::timestamp as market_timestamp,
v:symbol::string as symbol,
v:fetched_at::timestamp as fetched_at
from {{source('raw', 'raw_stock_quotes')}}
