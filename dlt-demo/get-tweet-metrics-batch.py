# Databricks notebook source
!/databricks/python3/bin/python -m pip install --upgrade pip
!pip install tweepy

# COMMAND ----------

import tweepy
import datetime
import pandas as pd

# COMMAND ----------

query = "select id as tweet_id from dbt_sdurry.raw_tweets where created_at >= CURRENT_DATE-30"
df = sqlContext.sql(query)
tweet_list = list(df.select('tweet_id').toPandas()['tweet_id'])

# COMMAND ----------

# MAGIC %sql
# MAGIC select id as tweet_id from dbt_sdurry.raw_tweets where created_at >= CURRENT_DATE-30

# COMMAND ----------

def divide_chunks(l, n):
    
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]

# COMMAND ----------

# We built chunks for 100 tweets to leverage the get_tweets endpoint
# https://docs.tweepy.org/en/stable/client.html#tweepy.Client.get_tweets
list_chunks = list(divide_chunks(tweet_list, 100))

# COMMAND ----------

bearer_token = dbutils.secrets.get(scope="sdurry-test", key="twitterbearer")
client = tweepy.Client(bearer_token)

tweet_stack = []
for x in range(len(list_chunks)):
    tweets_resp = client.get_tweets(ids=list_chunks[x], tweet_fields=["public_metrics"])
    tweet_stack.append(tweets_resp)

# COMMAND ----------

df_tweets = pd.DataFrame()

for resp in tweet_stack:
    
    for tweet in resp.data:
        df_tweets = df_tweets.append(pd.json_normalize(tweet.data), ignore_index = True)

# Later we want to have an updated at field
df_tweets['updated_at'] = datetime.datetime.now()

# COMMAND ----------

df_tweets.head(5)

# COMMAND ----------

dirname = "/dbfs/FileStore/sdurry/tweet_metrics_batch"
dt = datetime.datetime.now()
file_timestamp = dt.strftime("%y%m%d%H%M%S")
fname = dirname + '/tweets' + file_timestamp + '.csv'
df_tweets.to_csv(fname)

# COMMAND ----------

dbutils.notebook.exit("stop")

# COMMAND ----------


