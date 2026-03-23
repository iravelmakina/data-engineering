-- Test: fct_orders total_amount_usd should not be negative
select
  order_id,
  total_amount_usd
from {{ ref('fct_orders') }}
where total_amount_usd < 0
