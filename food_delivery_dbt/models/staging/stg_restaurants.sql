with source as (

    select * from {{ ref('raw_restaurants') }}

),

renamed as (

    select
        restaurant_id,
        {{ standardize_text('name') }}          as restaurant_name,
        {{ standardize_text('cuisine_type') }}  as cuisine_type,
        {{ standardize_text('city') }}          as city,
        cast(rating as decimal(3, 2))           as rating,
        opened_date,
        is_active

    from source

)

select * from renamed