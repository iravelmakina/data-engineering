{{
    config(
        materialized='table',
        unique_key='signup_date',
        on_schema_change='fail'
    )
}}

with customer_first_order as (

    select
        cast(customer_id as string) as customer_id,
        min(cast(ordered_at as date)) as first_order_date
    from {{ ref('fct_orders') }}
    group by 1

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
