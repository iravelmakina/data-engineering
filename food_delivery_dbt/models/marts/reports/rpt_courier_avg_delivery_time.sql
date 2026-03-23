{{
    config(
        materialized='table',
        unique_key=['courier_id', 'delivery_date'],
        on_schema_change='fail'
    )
}}

with courier_daily_deliveries as (

    select
        courier_id,
        cast(delivery_date as date) as delivery_date,
        count(delivery_id) as total_deliveries,
        avg(delivery_duration_min) as avg_delivery_duration_min
    from {{ ref('fct_deliveries') }}
    where delivery_status = 'delivered' -- Only consider successful deliveries
    group by courier_id, 2

),

overall_avg as (

    select
        avg(delivery_duration_min) as overall_avg_delivery_duration_min
    from {{ ref('fct_deliveries') }}
    where delivery_status = 'delivered'

),

final as (

    select
        cast(courier_id as string) as courier_id,
        delivery_date,
        total_deliveries,
        avg_delivery_duration_min,
        avg_delivery_duration_min - oa.overall_avg_delivery_duration_min as diff_from_overall_avg_min
    from courier_daily_deliveries cdd
    cross join overall_avg oa

)

select * from final
