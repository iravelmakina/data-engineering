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
        cast(delivered_at as date)  as delivery_date,
        count(delivery_id)          as total_deliveries,
        avg(delivery_duration_min)  as average_delivery_duration_min
    from {{ ref('fct_deliveries') }}
    where delivery_status = 'delivered' -- Only consider successful deliveries
    group by courier_id, 2

),

overall_average as (

    select
        avg(delivery_duration_min) as overall_average_delivery_duration_min
    from {{ ref('fct_deliveries') }}
    where delivery_status = 'delivered'

),

final as (

    select
        cast(courier_daily_deliveries.courier_id as string)                                       as courier_id,
        courier_daily_deliveries.total_deliveries,
        courier_daily_deliveries.average_delivery_duration_min,
        courier_daily_deliveries.average_delivery_duration_min
            - overall_average.overall_average_delivery_duration_min                               as difference_from_overall_average_min,
        courier_daily_deliveries.delivery_date
    from courier_daily_deliveries
    cross join overall_average

)

select * from final
