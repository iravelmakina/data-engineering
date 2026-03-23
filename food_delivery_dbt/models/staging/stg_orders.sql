with source as (

    select * from {{ ref('raw_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        restaurant_id,
        order_timestamp,
        {{ standardize_text('status') }}          as status,
        cast(total_amount_usd as decimal(10, 2))  as total_amount_usd,
        {{ standardize_text('payment_method') }}  as payment_method

    from source

)

select * from renamed