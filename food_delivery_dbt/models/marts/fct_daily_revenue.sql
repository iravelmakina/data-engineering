{{
    config(
        materialized='incremental',
        unique_key='revenue_date',
        on_schema_change='fail',
        incremental_strategy='delete+insert',
        incremental_predicates=[
            "DBT_INCREMENTAL_TARGET.revenue_date >= current_date - interval '7 days'"
        ]
    )
}}

with daily_orders as (

    select
        cast(ordered_at as date) as order_date,
        sum(total_amount_usd)   as daily_revenue_usd
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
        -- Reprocess the last 7 days on every run so late-arriving orders
        -- get folded into the aggregate. Older days stay frozen.
        -- The matching incremental_predicates in the config ensure the
        -- destination DELETE only scans the same 7-day window.
        where order_date >= current_date - interval '7 days'
    {% endif %}

)

select * from final
