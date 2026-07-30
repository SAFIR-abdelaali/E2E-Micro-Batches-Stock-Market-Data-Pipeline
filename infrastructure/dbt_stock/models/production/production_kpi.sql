select
    symbol,
    current_price,
    change_amount,
    change_percent
from (
    select *,
           row_number() over (partition by symbol order by fetched_at desc) as rn
    from {{ ref('cleaned_stg_stock_quotes') }}
) t
where rn = 1 