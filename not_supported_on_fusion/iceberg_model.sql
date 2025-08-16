{{
    config(
        materialized= 'table',
        catalog ='amy_catalog '
    )
}}

select * from {{ ref('fct_customers') }}
