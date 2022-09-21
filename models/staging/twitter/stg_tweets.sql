with

source__filtered as (

    select 
        *
    from {{ source('twitter','tweets')}}
    where lang IN ('en', 'de', 'es', 'fr', 'it')

),

clean_field_values as (

    select 
        conversation_id as tweet_id
        ,author_id
        ,source as source_name
        ,case
            when lang = 'de' then 'German'
            when lang = 'es' then 'Spanish'
            when lang = 'fr' then 'French'
            when lang = 'it' then 'Italian'
            when lang = 'en' then 'English'
        else 'error' end as language_name
        ,text as tweet_text
        ,cast(created_at as timestamp) as created_at
        ,row_number() over (
            partition by conversation_id 
            order by cast(created_at as timestamp) asc
        ) as tweet_row_index
    from source__filtered

),

filter_duplicates as (

    select *
    from clean_field_values
    where tweet_row_index = 1

)

select *
from filter_duplicates
