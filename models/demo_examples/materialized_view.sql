{{
    config(
        materialized='materialized_view'
    )
}}

select * from {{ ref('dim_customers') }}