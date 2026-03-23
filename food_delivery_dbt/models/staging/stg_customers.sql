with source as (

    select * from {{ ref('raw_customers') }}

),

renamed as (

    select
        customer_id,
        {{ standardize_text('first_name') }} as first_name,
        {{ standardize_text('last_name') }}  as last_name,
        {{ standardize_text('email') }}      as email,
        phone,
        signup_date,
        {{ standardize_text('city') }}       as city

    from source

)

select * from renamed