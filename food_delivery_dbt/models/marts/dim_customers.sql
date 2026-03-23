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
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        c.phone_number,
        c.city,
        c.signup_date,
        extract(year from c.signup_date)            as signup_year,
        coalesce(o.total_orders, 0)                 as total_orders,
        coalesce(o.lifetime_revenue_usd, 0)         as lifetime_revenue_usd,
        o.first_order_at,
        o.last_order_at
    from customers c
    left join orders_agg o on c.customer_id = o.customer_id

)

select * from final
