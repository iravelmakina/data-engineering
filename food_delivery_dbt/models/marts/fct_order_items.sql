{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='fail'
    )
}}

with new_order_items as (

    select *
    from {{ ref('stg_order_items') }}

    {% if is_incremental() %}
        -- Filter for new order items based on their unique ID.
        -- This ensures that even if an order is old, any new line items
        -- added to it will be processed.
        where cast(order_item_id as integer) > (
            select coalesce(max(cast(order_item_id as integer)), 0) from {{ this }}
        )
    {% endif %}

),

orders as (

    select
        order_id,
        customer_id,
        restaurant_id,
        status,
        ordered_at
    from {{ ref('stg_orders') }}

),

menu_items as (

    select
        menu_item_id,
        item_name,
        category
    from {{ ref('stg_menu_items') }}

),

final as (

    select
        new_order_items.order_item_id,
        new_order_items.order_id,
        orders.customer_id,
        orders.restaurant_id,
        new_order_items.menu_item_id,
        menu_items.item_name,
        menu_items.category,
        orders.status as order_status,
        new_order_items.quantity,
        new_order_items.unit_price_usd,
        new_order_items.line_total_usd,
        orders.ordered_at
    from new_order_items
    join orders      on new_order_items.order_id = orders.order_id
    left join menu_items on new_order_items.menu_item_id = menu_items.menu_item_id

)

select * from final
