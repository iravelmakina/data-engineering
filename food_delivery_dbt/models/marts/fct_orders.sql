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
        o.order_id,
        o.customer_id,
        o.restaurant_id,
        o.status,
        o.payment_method,
        o.total_amount_usd,
        coalesce(oi.item_count, 0)         as item_count,
        coalesce(oi.items_subtotal_usd, 0) as items_subtotal_usd,
        d.delivery_status,
        d.delivery_duration_min,
        d.distance_km,
        cast(o.ordered_at as date)         as order_date,
        o.ordered_at

    from orders o
    left join order_items oi on o.order_id = oi.order_id
    left join deliveries d   on o.order_id = d.order_id

)

select * from final
