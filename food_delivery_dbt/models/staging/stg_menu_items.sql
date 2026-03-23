with source as (

    select * from {{ ref('raw_menu_items') }}

),

renamed as (

    select
        menu_item_id,
        restaurant_id,
        {{ standardize_text('item_name') }}    as item_name,
        {{ standardize_text('category') }}     as category,
        cast(price_usd as decimal(10, 2))      as price_usd,
        is_available

    from source

)

select * from renamed