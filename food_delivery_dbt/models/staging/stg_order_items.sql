with source as (

    select * from {{ ref('raw_order_items') }}

),

renamed as (

    select
        order_item_id,
        order_id,
        menu_item_id,
        quantity,
        cast(unit_price_usd as decimal(10, 2))   as unit_price_usd,
        quantity * cast(unit_price_usd as decimal(10, 2)) as line_total_usd

    from source

)

select * from renamed