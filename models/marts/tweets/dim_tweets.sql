with

tweets as (

    select * from {{ ref('stg_tweets') }}

), is_deleted as (

    select * from {{ ref('int_deleted_tweets') }}

), tweets_is_deleted__joined as (

    select
        tweets.*,
        ifnull(is_deleted.is_deleted, false) as is_deleted
    from tweets
    left join is_deleted
        on tweets.tweet_id = is_deleted.tweet_id

),enrich as (

    select 
        tweet_id
        ,author_id
        ,source_name
        ,language_name
        ,created_at
        ,tweet_text
        ,regexp_replace(tweet_text, '(\#[0-9a-zA-Z]+|\@[0-9a-zA-Z]+|https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*))', '') as clean_tweet_text
        ,regexp_extract_all(tweet_text,'(https?:\\/\\/(?:www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{1,256}\\.[a-zA-Z0-9()]{1,6}\\b(?:[-a-zA-Z0-9()@:%_\\+.~#?&\\/=]*))',0) as links
        ,regexp_extract_all(tweet_text,'(\#[0-9a-zA-Z]+)',0) as hashtags
	    ,regexp_extract_all(tweet_text,'(\@[0-9a-zA-Z]+)',0) as mentions
        ,is_deleted
    from tweets_is_deleted__joined

), calculate as (

    select
        *
	    ,array_size(links) as link_count
        ,array_size(mentions) as mention_count
	    ,array_size(hashtags) as hashtag_count
    from enrich

)

select *
from calculate
