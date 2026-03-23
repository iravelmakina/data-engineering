with items as (

    select * from {{ ref('stg_menu_items') }}

),

sales as (

    select
        menu_item_id,
        sum(quantity)        as total_quantity_sold,
        sum(line_total_usd)  as total_revenue_usd
    from {{ ref('stg_order_items') }}
    group by 1

),

final as (

    select
        i.menu_item_id,
        i.restaurant_id,
        i.item_name,
        i.category,
        i.price_usd,
        i.is_available,
        coalesce(s.total_quantity_sold, 0)  as total_quantity_sold,
        coalesce(s.total_revenue_usd, 0)    as total_revenue_usd
    from items i
    left join sales s on i.menu_item_id = s.menu_item_id

)

select * from final
