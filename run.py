#!/usr/bin/python
# Python 3.5.2
# run.py
# contents of stream_process_tweets.py (see script for more details)
# attempting to consolidate to make part of scheduled process for heroku app
# see https://bigishdata.com/2016/12/15/running-python-background-jobs-with-heroku/
# for how to run locally
# see https://github.com/yuvadm/heroku-periodical
# for (perhaps) another solution using Celery


import tweepy as ty
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import StreamListener
import os
from dotenv import load_dotenv, find_dotenv # for dealing with environmental variables
import yweather # can use to allow passing specific location to Twitter for trends instead of "United States"
import pandas as pd
import datetime
import ujson
import boto3
from io import StringIO

class StdOutListener(StreamListener):

    def __init__(self, api=None):
        super(StdOutListener, self).__init__()
        self.num_tweets = 0
        self.tweets_list = []
        self.time = datetime.datetime.now()

    def on_data(self, data):
        if self.num_tweets < 7500 and (datetime.datetime.now() < self.time + datetime.timedelta(hours=1)):
        	# ensures it won't run longer than one hour per attempt
            try:
            	self.tweets_list.append(ujson.loads(data))
            	self.num_tweets += 1
            	return True
            except:
            	pass
            	return True
        else:
        	return False

    def on_error(self, status):
        print("Error with code: %s", status)
        return True

    def on_timeout(self):
        print("Timeout...")
        return True

def stream_and_process_trends():
	# credentials: twitter, aws
	# these fields are for running on Heroku
	consumer_key = os.environ["TW_CONSUMER"]
	consumer_secret = os.environ["TW_CONSUMER_SECRET"]
	access_token = os.environ["TW_ACCESS"]
	access_token_secret = os.environ["TW_ACCESS_SECRET"]
	aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
	aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
	aws_twitter_bucket = os.environ["AWS_TWITTER_BUCKET"]
	# these fields are for local testing (it didn't run on Heroku without using the above credentialing)
	# load_dotenv(find_dotenv())
	# consumer_key = os.environ.get("TW_CONSUMER")
	# consumer_secret = os.environ.get("TW_CONSUMER_SECRET")
	# access_token = os.environ.get("TW_ACCESS")
	# access_token_secret = os.environ.get("TW_ACCESS_SECRET")
	# aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
	# aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
	# aws_twitter_bucket = os.environ.get("AWS_TWITTER_BUCKET")
	auth = ty.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	s3 = boto3.resource("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	# get trends
	api = ty.API(auth)
	available = api.trends_available()
	trends = api.trends_place(23424977)
	trends_list = [item["name"] for item in trends[0]["trends"]]
	# create listener and stream trends, filter results into list to avoid Key Error
	l = StdOutListener()
	stream = ty.Stream(auth, l)
	stream.filter(track=trends_list)
	tweets = [tweet for tweet in l.tweets_list if (type(tweet) == dict) and ("text" in tweet.keys())]
	# write create dataframe of tweets
	tweets_df = pd.DataFrame([[item["text"], item["user"]["id_str"], item["user"]["screen_name"], item["user"]["statuses_count"],
	item["user"]["followers_count"], item["user"]["favourites_count"], item["user"]["friends_count"]] for item in tweets], 
	columns = ["text", "id_str", "screen_name", "statuses_count", "followers_count", "favourites_count", "friends_count"])
	# write tweets datafram to a file on s3 bucket, file named for UTC datetime it was created
	csv_buffer = StringIO()
	tweets_df.to_csv(csv_buffer)
	now = datetime.datetime.now()
	s3.Object(aws_twitter_bucket, "%s.csv" % now.utcnow().strftime("%y-%m-%d_%H:%M:%S")).put(Body=csv_buffer.getvalue())


