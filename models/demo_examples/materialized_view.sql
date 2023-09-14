{{
    config(
        materialized='materialized_view'
    )
}}

select * from {{ ref('streaming_table') }}