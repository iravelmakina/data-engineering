{{
    config(
        materialized='incremental',
        unique_key='order_id',
        on_schema_change='fail'
    )
}}

with orders as (

    select * from {{ ref('stg_orders') }}

    {% if is_incremental() %}
        where ordered_at > (
            select coalesce(max(ordered_at), '1900-01-01'::timestamp) from {{ this }}
        )
    {% endif %}

),

order_items as (

    select
        order_id,
        sum(quantity)        as item_count,
        sum(line_total_usd)  as items_subtotal_usd
    from {{ ref('stg_order_items') }}
    group by 1

),

deliveries as (

    select
        order_id,
        delivery_status,
        delivery_duration_min,
        distance_km
    from {{ ref('stg_deliveries') }}

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.restaurant_id,
        orders.status,
        orders.payment_method,
        deliveries.delivery_status,
        orders.total_amount_usd,
        coalesce(order_items.item_count, 0)         as item_count,
        coalesce(order_items.items_subtotal_usd, 0) as items_subtotal_usd,
        deliveries.delivery_duration_min,
        deliveries.distance_km,
        cast(orders.ordered_at as date)             as order_date,
        orders.ordered_at

    from orders
    left join order_items on orders.order_id = order_items.order_id
    left join deliveries  on orders.order_id = deliveries.order_id

)

select * from final
