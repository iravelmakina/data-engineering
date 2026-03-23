with source as (

    select * from {{ source('raw', 'raw_deliveries') }}

),

renamed as (

    select
        cast(delivery_id as string)               as delivery_id,
        cast(order_id as string)                  as order_id,
        {{ dbt_utils.generate_surrogate_key(['courier_name']) }} as courier_id,
        {{ standardize_text('courier_name') }}     as courier_name,
        {{ standardize_text('courier_phone') }}    as courier_phone_number,
        {{ standardize_text('delivery_status') }}  as delivery_status,
        cast(distance_km as decimal(6, 2))         as distance_km,
        date_diff('minute', pickup_timestamp, delivery_timestamp) as delivery_duration_min,
        pickup_timestamp                           as picked_up_at,
        delivery_timestamp                         as delivered_at

    from source

)

select * from renamed
