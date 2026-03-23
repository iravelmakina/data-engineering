{{
    config(
        materialized='table',
        unique_key=['revenue_date', 'restaurant_id'],
        on_schema_change='fail'
    )
}}

with restaurant_daily_revenue as (

    select
        restaurant_id,
        revenue_date,
        daily_revenue_usd
    from {{ ref('fct_restaurant_daily') }}

),

final as (

    select
        restaurant_id,
        revenue_date,
        daily_revenue_usd,
        rank() over (partition by revenue_date order by daily_revenue_usd desc) as daily_revenue_rank
    from restaurant_daily_revenue

)

select * from final
