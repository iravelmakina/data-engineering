with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed as (

    select
        cast(order_id as string)                 as order_id,
        cast(customer_id as string)              as customer_id,
        cast(restaurant_id as string)            as restaurant_id,
        {{ standardize_text('status') }}         as status,
        {{ standardize_text('payment_method') }} as payment_method,
        cast(total_amount_usd as decimal(10, 2)) as total_amount_usd,
        ordered_at

    from source

)

select * from renamed
