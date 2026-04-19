-- Test: dim_restaurants is_top_performer consistency
with restaurants as (
    select
        restaurant_id,
        is_top_performer
    from {{ ref('dim_restaurants') }}
),

calculated_revenue as (
    select
        restaurant_id,
        sum(total_amount_usd) filter (where status = 'delivered') as total_revenue_usd
    from {{ ref('stg_orders') }}
    group by 1
),

final as (
    select
        restaurants.restaurant_id,
        restaurants.is_top_performer,
        coalesce(calculated_revenue.total_revenue_usd, 0)         as calculated_total_revenue_usd,
        (coalesce(calculated_revenue.total_revenue_usd, 0) >= 2500) as expected_is_top_performer
    from restaurants
    left join calculated_revenue on restaurants.restaurant_id = calculated_revenue.restaurant_id
)

select
    restaurant_id
from final
where is_top_performer != expected_is_top_performer
