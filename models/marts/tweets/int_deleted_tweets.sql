{{ config(
    materialized='view') 
    }}

with

all_tweets as (

    select * from {{ ref('stg_tweets') }}

),

tweet_metrics as (

    select * from {{ ref('fct_tweets') }}

),

tweets__joined as (

    select
       all_tweets.tweet_id,
       tweet_metrics.tweet_id is null as is_deleted
    from all_tweets
    left join tweet_metrics
        on all_tweets.tweet_id = tweet_metrics.tweet_id

)

select *
from tweets__joined
