{#
    Courier dimension. The surrogate key is generated in stg_deliveries
    via dbt_utils.generate_surrogate_key so every downstream model
    (dim_couriers + fct_deliveries) derives it from the same source of
    truth and we avoid a fact -> dim dependency.
#}

with deliveries as (

    select * from {{ ref('stg_deliveries') }}
    where courier_name is not null

),

courier_agg as (

    select
        courier_id,
        courier_name,
        max(courier_phone_number)                                    as courier_phone_number,
        count(*)                                                     as total_deliveries,
        avg(distance_km)                                             as avg_distance_km,
        avg(delivery_duration_min)                                   as avg_duration_min,
        sum(case when delivery_status = 'failed' then 1 else 0 end)  as failed_deliveries
    from deliveries
    group by courier_id, courier_name

),

final as (

    select
        courier_id,
        courier_name,
        courier_phone_number,
        failed_deliveries,
        total_deliveries,
        cast(avg_distance_km as decimal(6, 2))                            as avg_distance_km,
        cast(avg_duration_min as decimal(6, 2))                           as avg_duration_min,
        cast(failed_deliveries * 1.0 / total_deliveries as decimal(5, 4)) as failure_rate
    from courier_agg

)

select * from final
