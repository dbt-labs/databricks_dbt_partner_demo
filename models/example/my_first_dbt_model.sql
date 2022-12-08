/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

with source_data as (

    select 
        1 as id
        ,'Hallo dbt Fans' as my_string

)

select *
from source_data
