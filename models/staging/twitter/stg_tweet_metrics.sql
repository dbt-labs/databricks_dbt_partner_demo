with

source as (

    select * from {{ source('twitter','tweet_metrics')}}

),

clean_add_columns as (

    select 
        id as tweet_id,
        `public_metrics.like_count` as likes,
        `public_metrics.retweet_count` as retweets,
        `public_metrics.reply_count` as replies,
        `public_metrics.quote_count` as quotes,
        `public_metrics.like_count` + `public_metrics.retweet_count` + `public_metrics.reply_count` + `public_metrics.quote_count` as total_engagements,
        cast(updated_at as timestamp) as updated_at,
        -- The table is append only so we need to index the entries of tweets to filter on the latest 
        row_number() over (
            partition by id 
            order by cast(updated_at as timestamp) desc
        ) as tweet_row_index
    from source

), filter_latest_entry as (

    select 
        *
    from clean_add_columns
    where tweet_row_index = 1

)

select *
from filter_latest_entry
