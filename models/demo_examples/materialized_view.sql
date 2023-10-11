{{
    config(
        materialized='materialized_view',
        enabled = false
    )
}}

select * from {{ ref('dim_customers') }}