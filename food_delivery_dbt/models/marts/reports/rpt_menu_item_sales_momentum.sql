{{
    config(
        materialized='table',
        unique_key=['menu_item_id', 'order_date'],
        on_schema_change='fail'
    )
}}

with menu_item_daily_sales as (

    select
        menu_item_id,
        cast(ordered_at as date) as order_date,
        sum(line_total_usd) as daily_sales_usd
    from {{ ref('fct_order_items') }}
    group by menu_item_id, 2

),

final as (

    select
        cast(menu_item_id as string) as menu_item_id,
        order_date,
        daily_sales_usd,
        lag(daily_sales_usd, 1, 0) over (
            partition by menu_item_id
            order by order_date
        ) as previous_day_sales_usd,
        (daily_sales_usd - lag(daily_sales_usd, 1, 0) over (
            partition by menu_item_id
            order by order_date
        )) / nullif(lag(daily_sales_usd, 1, 1) over (
            partition by menu_item_id
            order by order_date
        ), 0) as daily_sales_growth_rate
    from menu_item_daily_sales

)

select * from final
