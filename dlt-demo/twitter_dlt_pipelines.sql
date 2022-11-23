-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE raw_tweets__dlt
COMMENT "a streaming data set of twitter api data"
TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
AS SELECT
  *
FROM cloud_files("dbfs:/FileStore/sdurry/tweets", "json");

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_tweet_metrics__dlt
COMMENT "a batch of tweet metrics ingested every day"
AS SELECT
  *
FROM cloud_files("dbfs:/FileStore/sdurry/tweet_metrics_batch", "csv")
-- AUTO LOADER SYNTAX https://docs.databricks.com/ingestion/auto-loader/dlt.html#language-sql
;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE stg_tweets__dlt(
  CONSTRAINT no_missing_records EXPECT (tweet_id IS NOT NULL) ON VIOLATION DROP ROW
  -- for a uniquness test you need to write a separate model
)
COMMENT "A subset of columns of raw_tweets table"
AS  
WITH source__filtered as (
SELECT *
FROM Live.raw_tweets__dlt
WHERE lang IN ('en', 'de', 'es', 'fr', 'it')
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
from filter_duplicates;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE stg_tweet_metrics__dlt(
  CONSTRAINT no_missing_records EXPECT (tweet_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "A subset of columns of raw_tweets table"
AS  
with

source as (

    select * from live.raw_tweet_metrics__dlt

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
from filter_latest_entry;

-- COMMAND ----------

CREATE TEMPORARY LIVE VIEW int_deleted_tweets__dlt
COMMENT "A subset of columns of raw_tweets table"
AS  
with

all_tweets as (

    select * from live.stg_tweets__dlt

),

tweet_metrics as (

    select * from live.stg_tweet_metrics__dlt

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
from tweets__joined;

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE dim_tweets__dlt
COMMENT "A subset of columns of raw_tweets table"
AS  
with

tweets as (

    select * from live.stg_tweets__dlt

), is_deleted as (

    select * from live.int_deleted_tweets__dlt

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
from calculate;
