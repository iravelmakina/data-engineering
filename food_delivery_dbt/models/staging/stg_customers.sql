with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        cast(customer_id as string)          as customer_id,
        {{ standardize_text('first_name') }} as first_name,
        {{ standardize_text('last_name') }}  as last_name,
        {{ standardize_text('email') }}      as email,
        {{ standardize_text('phone') }}      as phone_number,
        {{ standardize_text('city') }}       as city,
        signup_date

    from source

)

select * from renamed
