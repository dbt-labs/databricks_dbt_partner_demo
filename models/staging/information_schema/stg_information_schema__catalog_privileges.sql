with 

source as (

    select * from {{ source('information_schema', 'catalog_privileges') }}

),

renamed as (

    select
        *
    from source

)

select * from renamed
