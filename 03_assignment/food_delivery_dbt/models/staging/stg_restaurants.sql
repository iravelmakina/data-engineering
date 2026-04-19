with source as (

    select * from {{ source('raw', 'raw_restaurants') }}

),

renamed as (

    select
        cast(restaurant_id as string)           as restaurant_id,
        {{ standardize_text('name') }}          as restaurant_name,
        {{ standardize_text('cuisine_type') }}  as cuisine_type,
        {{ standardize_text('city') }}          as city,
        cast(rating as decimal(3, 2))           as rating,
        is_active,
        opened_date

    from source

)

select * from renamed
