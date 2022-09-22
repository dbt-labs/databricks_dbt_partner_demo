with 

dim_tweets as (

    select 
        tweet_id
        ,author_id
        ,source_name
        ,language_name
        ,created_at
        ,links
        ,hashtags
        ,mentions 
    from {{ ref('dim_tweets') }}
    where is_deleted is false

), 

fct_tweets as (

    select * from {{ ref('fct_tweets') }}

),

dim_fct__joined as (

    select
        dim_tweets.tweet_id
        ,dim_tweets.author_id
        ,dim_tweets.source_name
        ,dim_tweets.language_name
        ,dim_tweets.created_at
        ,array_size(links) as links
        ,array_size(hashtags) as hashtags
        ,array_size(mentions) as mentions
        ,ifnull(fct_tweets.likes,0) as likes
        ,ifnull(fct_tweets.retweets,0) as retweets
        ,ifnull(fct_tweets.replies,0) as replies
        ,ifnull(fct_tweets.quotes,0) as quotes
        ,ifnull(fct_tweets.total_engagements,0) as total_engagements
    from dim_tweets
    left join fct_tweets using (tweet_id)

)

select *
from dim_fct__joined
