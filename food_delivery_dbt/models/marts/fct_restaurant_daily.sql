{{
    config(
        materialized='incremental',
        unique_key=['restaurant_id', 'revenue_date'],
        on_schema_change='fail',
        incremental_strategy='merge',
        incremental_predicates=["DBT_INTERNAL_DEST.revenue_date >= (select min(revenue_date) from " ~ ref('fct_orders') ~ " where order_date >= current_date - interval '1 week')"]
    )
}}

with restaurant_daily_orders as (

    select
        restaurant_id,
        cast(order_timestamp as date) as order_date,
        sum(total_amount_usd) as daily_revenue_usd
    from {{ ref('fct_orders') }}
    group by restaurant_id, cast(order_timestamp as date)

),

final as (

    select
        restaurant_id,
        order_date as revenue_date,
        daily_revenue_usd
    from restaurant_daily_orders

    {% if is_incremental() %}
        where order_date > (
            select coalesce(max(revenue_date), '1900-01-01'::date) from {{ this }}
        )
    {% endif %}

)

select * from final
