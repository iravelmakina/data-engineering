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
        where delivered_at > (
            select coalesce(max(delivered_at), '1900-01-01'::timestamp) from {{ this }}
        )
    {% endif %}

),

final as (

    select
        delivery_id,
        order_id,
        courier_id,
        delivery_status,
        delivery_duration_min,
        distance_km,
        delivered_at
    from deliveries

)

select * from final
