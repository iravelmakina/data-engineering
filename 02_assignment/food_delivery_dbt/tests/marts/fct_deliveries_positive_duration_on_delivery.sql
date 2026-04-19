-- Test: fct_deliveries delivery_duration_min should be positive when delivery_status is 'delivered'
select
  delivery_id,
  delivery_duration_min,
  delivery_status
from {{ ref('fct_deliveries') }}
where delivery_status = 'delivered' and delivery_duration_min <= 0
