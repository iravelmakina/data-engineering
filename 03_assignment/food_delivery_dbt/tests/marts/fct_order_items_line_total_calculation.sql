-- Test: fct_order_items line_total_usd should equal quantity * unit_price_usd
select
  order_item_id,
  quantity,
  unit_price_usd,
  line_total_usd
from {{ ref('fct_order_items') }}
where round(line_total_usd, 2) != round(quantity * unit_price_usd, 2)
