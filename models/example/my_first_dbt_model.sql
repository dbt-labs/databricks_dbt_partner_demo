/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with source_data as (

    select 
        1 as id
        ,'Hello databricks' as my_string
        ,2 as my_new_column

)

select *
from source_data
