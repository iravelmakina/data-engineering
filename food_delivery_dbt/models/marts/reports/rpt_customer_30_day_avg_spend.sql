{{
    config(
        materialized='table',
        unique_key=['customer_id', 'order_date'],
        on_schema_change='fail'
    )
}}

with customer_daily_spend as (

    select
        customer_id,
        cast(order_timestamp as date) as order_date,
        sum(total_amount_usd) as daily_spend_usd
    from {{ ref('fct_orders') }}
    group by customer_id, 2

),

final as (

    select
        customer_id,
        order_date,
        daily_spend_usd,
        avg(daily_spend_usd) over (
            partition by customer_id
            order by order_date
            rows between 29 preceding and current row
        ) as rolling_30_day_avg_spend_usd
    from customer_daily_spend

)

select * from final
