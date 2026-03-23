with customers as (

    select * from {{ ref('stg_customers') }}

),

orders_agg as (

    select
        customer_id,
        count(*)                                                  as total_orders,
        sum(total_amount_usd) filter (where status = 'delivered') as lifetime_revenue_usd,
        min(ordered_at)                                           as first_order_at,
        max(ordered_at)                                           as last_order_at
    from {{ ref('stg_orders') }}
    group by 1

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customers.email,
        customers.phone_number,
        customers.city,
        extract(year from customers.signup_date)    as signup_year,
        coalesce(orders_agg.total_orders, 0)        as total_orders,
        coalesce(orders_agg.lifetime_revenue_usd, 0) as lifetime_revenue_usd,
        customers.signup_date,
        orders_agg.first_order_at,
        orders_agg.last_order_at
    from customers
    left join orders_agg on customers.customer_id = orders_agg.customer_id

)

select * from final
