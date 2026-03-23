with menu_items as (

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
        menu_items.menu_item_id,
        menu_items.restaurant_id,
        menu_items.item_name,
        menu_items.category,
        menu_items.price_usd,
        coalesce(sales.total_quantity_sold, 0)  as total_quantity_sold,
        coalesce(sales.total_revenue_usd, 0)    as total_revenue_usd,
        menu_items.is_available
    from menu_items
    left join sales on menu_items.menu_item_id = sales.menu_item_id

)

select * from final
