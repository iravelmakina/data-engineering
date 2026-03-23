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
        r.restaurant_id,
        r.restaurant_name,
        r.cuisine_type,
        r.city,
        r.rating,
        r.opened_date,
        r.is_active,
        coalesce(o.total_orders, 0)                                                as total_orders,
        coalesce(o.total_revenue_usd, 0)                                           as total_revenue_usd,
        coalesce(o.total_revenue_usd, 0) >= 2500                                   as is_top_performer
    from restaurants r
    left join orders_agg o on r.restaurant_id = o.restaurant_id

)

select * from final
