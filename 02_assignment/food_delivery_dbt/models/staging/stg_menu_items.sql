with source as (

    select * from {{ source('raw', 'raw_menu_items') }}

),

renamed as (

    select
        cast(menu_item_id as string)           as menu_item_id,
        cast(restaurant_id as string)          as restaurant_id,
        {{ standardize_text('item_name') }}    as item_name,
        {{ standardize_text('category') }}     as category,
        cast(price_usd as decimal(10, 2))      as price_usd,
        is_available

    from source

)

select * from renamed
