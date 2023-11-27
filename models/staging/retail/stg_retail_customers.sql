{{
    config(
        materialized='table',
        databricks_compute= 'Compute1'
    )
}}

with source as (
    select * 
      from {{ source('retail', 'customers') }}
)

, renamed as (
    select customer_id          as customer_id
         , cast(tax_id as int)  as tax_id
         , tax_code             as tax_code
         , customer_name        as customer_name
         , state                as state
         , city                 as city
         , case when postcode like '%-%'
                then cast(left(postcode,5) as int)
                else cast(postcode as int)
            end                 as postcode
         , street               as street
         , case when number like '%.%'
                then cast(number as int)
                else number
            end                 as number              
         , unit                 as unit
         , region               as region
         , district             as district
         , cast(lon as double)  as longitude
         , cast(lat as double)  as latitude
         , ship_to_address      as ship_to_address
         , from_unixtime(valid_from,'yyyy-MM-dd')   as valid_from_date
         , from_unixtime(valid_to,'yyyy-MM-dd')     as valid_to_date
         , cast(units_purchased as int) as units_purchased
         , loyalty_segment      as loyalty_segment
      from source

)

, de_duped as (
    select *
      from renamed
     where valid_to_date is null
)

    select * 
      from de_duped
