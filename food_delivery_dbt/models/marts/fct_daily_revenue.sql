{{
    config(
        materialized='incremental',
        unique_key='revenue_date',
        on_schema_change='fail'
    )
}}

with daily_orders as (

    select
        cast(order_timestamp as date) as order_date,
        sum(total_amount_usd) as daily_revenue_usd
    from {{ ref('fct_orders') }}
    group by 1

),

final as (

    select
        order_date as revenue_date,
        daily_revenue_usd,
        sum(daily_revenue_usd) over (order by order_date) as running_total_revenue_usd
    from daily_orders

    {% if is_incremental() %}
        where order_date > (
            select coalesce(max(revenue_date), '1900-01-01'::date) from {{ this }}
        )
    {% endif %}

)

select * from final
