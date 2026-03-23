{{
    config(
        materialized='incremental',
        unique_key='order_item_id',
        on_schema_change='fail'
    )
}}

{# Line-level fact. Incremental filter is on the parent orders timestamp,
   since order_items inherit their date from the order. #}

with new_orders as (

    select order_id, customer_id, restaurant_id, order_timestamp, status
    from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        where order_timestamp > (
            select coalesce(max(order_timestamp), '1900-01-01'::timestamp) from {{ this }}
        )
    {% endif %}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

menu as (

    select menu_item_id, item_name, category from {{ ref('stg_menu_items') }}

),

final as (

    select
        oi.order_item_id,
        oi.order_id,
        o.customer_id,
        o.restaurant_id,
        oi.menu_item_id,
        m.item_name,
        m.category,
        o.order_timestamp,
        oi.quantity,
        oi.unit_price_usd,
        oi.line_total_usd,
        o.status as order_status
    from new_orders o
    join order_items oi on o.order_id = oi.order_id
    left join menu m    on oi.menu_item_id = m.menu_item_id

)

select * from final
