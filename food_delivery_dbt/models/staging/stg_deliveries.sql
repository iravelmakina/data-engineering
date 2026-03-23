with source as (

    select * from {{ ref('raw_deliveries') }}

),

renamed as (

    select
        delivery_id,
        order_id,
        {{ standardize_text('courier_name') }}    as courier_name,
        courier_phone,
        pickup_timestamp,
        delivery_timestamp,
        {{ standardize_text('delivery_status') }} as delivery_status,
        cast(distance_km as decimal(6, 2))        as distance_km,
        date_diff('minute', pickup_timestamp, delivery_timestamp) as delivery_duration_min

    from source

)

select * from renamed