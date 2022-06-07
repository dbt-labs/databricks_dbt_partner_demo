with source as (

    select * from {{ source('retail', 'customers') }}

),

renamed as (

    select customer_id
         , tax_id
         , tax_code
         , customer_name
         , state
         , city
         , postcode
         , street
         , number
         , unit
         , region
         , district
         , lon
         , lat
         , ship_to_address
         , valid_from
         , valid_to
         , units_purchased
         , loyalty_segment
      from source

)

select * from renamed