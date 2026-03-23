{{
    config(
        materialized='table',
        unique_key='signup_date',
        on_schema_change='fail'
    )
}}

with customer_first_order as (

    select
        customer_id,
        min(cast(order_timestamp as date)) as first_order_date
    from {{ ref('fct_orders') }}
    group by customer_id

),

daily_new_customers as (

    select
        first_order_date as signup_date,
        count(distinct customer_id) as new_customers
    from customer_first_order
    group by first_order_date

),

final as (

    select
        signup_date,
        new_customers,
        sum(new_customers) over (order by signup_date) as running_total_customers
    from daily_new_customers

)

select * from final
