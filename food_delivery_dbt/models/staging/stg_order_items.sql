with source as (

    select * from {{ source('raw', 'raw_order_items') }}

),

renamed as (

    select
        cast(order_item_id as string)           as order_item_id,
        cast(order_id as string)                as order_id,
        cast(menu_item_id as string)            as menu_item_id,
        quantity,
        cast(unit_price_usd as decimal(10, 2))  as unit_price_usd,
        cast(quantity * unit_price_usd as decimal(10, 2)) as line_total_usd

    from source

)

select * from renamed
