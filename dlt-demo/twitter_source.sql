-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE raw_tweets(
  CONSTRAINT valid_timestamp EXPECT (created_at > '2022-01-01')
)
COMMENT "a streaming data set of twitter api data"
TBLPROPERTIES (
   'delta.columnMapping.mode' = 'name',
   'delta.minReaderVersion' = '2',
   'delta.minWriterVersion' = '5')
AS SELECT
  *
FROM cloud_files("dbfs:/FileStore/sdurry/tweets", "json");

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE raw_tweet_metrics
COMMENT "a batch of tweet metrics ingested every day"
AS SELECT
  *
FROM cloud_files("dbfs:/FileStore/sdurry/tweet_metrics_batch", "csv");

-- From here on we choose to let DBT take over!
