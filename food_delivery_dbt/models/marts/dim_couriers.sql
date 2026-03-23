{#
    Couriers don't have an ID in the source data (just a name and phone).
    We derive a stable surrogate key by hashing the courier_name.
#}

with deliveries as (

    select * from {{ ref('stg_deliveries') }}
    where courier_name is not null

),

courier_agg as (

    select
        courier_name,
        max(courier_phone)                                                  as courier_phone,
        count(*)                                                            as total_deliveries,
        avg(distance_km)                                                    as avg_distance_km,
        avg(delivery_duration_min)                                          as avg_duration_min,
        sum(case when delivery_status = 'failed' then 1 else 0 end)         as failed_deliveries
    from deliveries
    group by 1

),

final as (

    select
        md5(courier_name)                                                   as courier_id,
        courier_name,
        courier_phone,
        total_deliveries,
        cast(avg_distance_km as decimal(6, 2))                              as avg_distance_km,
        cast(avg_duration_min as decimal(6, 2))                             as avg_duration_min,
        failed_deliveries,
        cast(failed_deliveries * 1.0 / total_deliveries as decimal(5, 4))   as failure_rate
    from courier_agg

)

select * from final
