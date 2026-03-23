{{
    config(
        materialized='incremental',
        unique_key='delivery_id',
        on_schema_change='fail'
    )
}}

with deliveries as (

    select *
    from {{ ref('stg_deliveries') }}

    {% if is_incremental() %}
        where delivery_timestamp > (
            select coalesce(max(delivery_timestamp), '1900-01-01'::timestamp) from {{ this }}
        )
    {% endif %}

),

couriers as (

    select
        courier_id,
        courier_name
    from {{ ref('dim_couriers') }}

),

final as (

    select
        d.delivery_id,
        d.order_id,
        c.courier_id, -- Get courier_id from dim_couriers
        d.delivery_timestamp,
        d.delivery_status,
        d.delivery_duration_min,
        d.distance_km
    from deliveries d
    left join couriers c on d.courier_name = c.courier_name

)

select * from final
