{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='fail'
    )
}}

{#
    This model is incrementally loaded based on new order_item_ids.
    We join to stg_orders to enrich the line item data with order-level details.
    The incremental filter is applied to stg_order_items to ensure all new line items are captured.
#}

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

menu as (

    select
        menu_item_id,
        item_name,
        category
    from {{ ref('stg_menu_items') }}

),

final as (

    select
        noi.order_item_id,
        noi.order_id,
        o.customer_id,
        o.restaurant_id,
        noi.menu_item_id,
        m.item_name,
        m.category,
        noi.quantity,
        noi.unit_price_usd,
        noi.line_total_usd,
        o.status as order_status,
        o.ordered_at
    from new_order_items noi
    join orders o on noi.order_id = o.order_id
    left join menu m    on noi.menu_item_id = m.menu_item_id

)

select * from final
