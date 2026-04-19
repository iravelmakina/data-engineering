with restaurants as (

    select * from {{ ref('stg_restaurants') }}

),

orders_agg as (

    select
        restaurant_id,
        count(*)                                                  as total_orders,
        sum(total_amount_usd) filter (where status = 'delivered') as total_revenue_usd
    from {{ ref('stg_orders') }}
    group by 1

),

final as (

    select
        restaurants.restaurant_id,
        restaurants.restaurant_name,
        restaurants.cuisine_type,
        restaurants.city,
        restaurants.rating,
        coalesce(orders_agg.total_orders, 0)                       as total_orders,
        coalesce(orders_agg.total_revenue_usd, 0)                  as total_revenue_usd,
        restaurants.is_active,
        coalesce(orders_agg.total_revenue_usd, 0) >= 2500          as is_top_performer,
        restaurants.opened_date
    from restaurants
    left join orders_agg on restaurants.restaurant_id = orders_agg.restaurant_id

)

select * from final
