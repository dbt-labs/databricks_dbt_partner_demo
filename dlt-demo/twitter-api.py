# Databricks notebook source
# Inspired by https://github.com/fmunz/twitter-dlt-huggingface/blob/main/Twitter-Stream-S3.py
!/databricks/python3/bin/python -m pip install --upgrade pip
!pip install tweepy jsonpickle

# COMMAND ----------

import os
import tweepy
import datetime
import jsonpickle

# COMMAND ----------

## Configure the Stream
bearer_token = dbutils.secrets.get(scope="sdurry-test", key="twitterbearer")
dbfs_dir = "/dbfs/FileStore/sdurry/tweets" 
## https://developer.twitter.com/en/docs/twitter-api/tweets/filtered-stream/integrate/build-a-rule
dbt_rule = "(@getdbt OR @dbt_labs OR @coalesceconf OR #coalesce OR #dbt OR #dbtlabs OR #dbtCoalesce OR elt OR etl OR analytics engineering OR data analyst OR data transformation) -is:retweet"
my_rule = dbt_rule
fields = "lang,geo,author_id,conversation_id,created_at,referenced_tweets,reply_settings,source,in_reply_to_user_id,non_public_metrics,organic_metrics,public_metrics"

# COMMAND ----------

class myStream(tweepy.StreamingClient):

    def __init__(self, bearer_token, dirname):
        tweepy.StreamingClient.__init__(self,bearer_token)
        self.dirname = dirname
        self.tweet_count = 0
        self.tweet_stack = []

        
    # called for every tweet
    def on_tweet(self, tweet):
        self.tweet_count = self.tweet_count + 1
        self.tweet_stack.append(tweet)

        # we don't want more than 50 tweets per run
        if (self.tweet_count == 50):
            stream.disconnect() 
        elif (self.tweet_count % 1 == 0):
            print(f"tweet {self.tweet_count} from stream: {tweet.text}")

        if (self.tweet_count % 10 == 0):
            self.write_file()
            self.tweet_stack = []

    def write_file(self):
        dt = datetime.datetime.now()
        file_timestamp = dt.strftime("%y%m%d%H%M%S")
        fname = self.dirname + '/tweets_' + str(file_timestamp) + '.json'
        open(fname, "x")
        
        print(f'writing tweets to: {fname}')
        
        with open(fname, 'w') as f:
          for tweet in self.tweet_stack:
            f.write(jsonpickle.encode(tweet, unpicklable=False) + '\n')
            
    def on_error(self, status_code):
        print("Error with code ", status_code)
        sys.exit()
        
    def on_connection_error(self, status_code):
        print("Error with code ", status_code)
        sys.exit()

# COMMAND ----------

stream = myStream(bearer_token, dbfs_dir)
try:      
    rules = stream.get_rules()
    if rules.data != None:
      for r in rules.data:
        stream.delete_rules(r.id)
    
    new_rule = stream.add_rules(tweepy.StreamRule(my_rule))
    
    print(f"rules set: {stream.get_rules()}")
    stream.filter(threaded=False, tweet_fields=fields)
    
    
except Exception as e:
    print("some error ", e)
    print("Writing out tweets file before I have to exit")
    stream.write_file()
    stream.disconnect()
finally:
    print("Downloaded tweets ", stream.tweet_count)

# COMMAND ----------

dbutils.notebook.exit("stop")
