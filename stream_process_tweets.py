#!/usr/bin/python
# Python 3

# this app asks twitter for current US trends, opens and closes a stream following those trends, 
# and writes the resulting set of tweets to a .csv on an Amazon S3 bucket

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

# credentials, four Twitter, two AWS
load_dotenv(find_dotenv())
consumer_key = os.environ.get("TW_CONSUMER")
consumer_secret = os.environ.get("TW_CONSUMER_SECRET")
access_token = os.environ.get("TW_ACCESS")
access_token_secret = os.environ.get("TW_ACCESS_SECRET")
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
aws_twitter_bucket = os.environ.get("AWS_TWITTER_BUCKET")
auth = ty.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# getting trends

def get_US_Twitter_trends():
    auth = ty.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = ty.API(auth)
    available = api.trends_available()
    return(api.trends_place(23424977))

###### yweather get WOEID function. No API keys required. Only used in a version where user
# can ask for trends in a specific location. current application is US trends only.
#
# def get_WOEID(location):
#     client = yweather.Client()
#     return(client.fetch_woeid(location))

# twitter streaming listener modified from adilmoujahid.com

class StdOutListener(StreamListener):

    def __init__(self, api=None):
        super(StdOutListener, self).__init__()
        self.num_tweets = 0
        self.tweets_list = []
        self.time = datetime.datetime.now()

    def on_data(self, data):
        if self.num_tweets < 7500 and (datetime.datetime.now() < self.time + datetime.timedelta(hours=1)):
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


def tweets_to_df(tweets_list):
	return(pd.DataFrame([[item["text"], item["user"]["id_str"], item["user"]["screen_name"], item["user"]["statuses_count"],
	item["user"]["followers_count"], item["user"]["favourites_count"], item["user"]["friends_count"]] for item in tweets_list], 
	columns = ["text", "id_str", "screen_name", "statuses_count", "followers_count", "favourites_count", "friends_count"]))



if __name__ == '__main__':

	trends = get_US_Twitter_trends()
	# trends_list = [trends[0]['trends'][i]['name'] for i in range(len(trends[0]['trends']))]
	trends_list = [item["name"] for item in trends[0]["trends"]]
	l = StdOutListener()
	stream = ty.Stream(auth, l)
	stream.filter(track=trends_list)
	tweets = l.tweets_list
	tweets_df = tweets_to_df(tweets)
	s3 = boto3.resource("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
	csv_buffer = StringIO()
	tweets_df.to_csv(csv_buffer)
	now = datetime.datetime.now()
	s3.Object(aws_twitter_bucket, "%s.csv" % now.utcnow().strftime("%y-%m-%d_%H:%M:%S")).put(Body=csv_buffer.getvalue())
    # this seems to work locally; it gives a .csv which is readable for my Shiny app
    # boto3 code adapted from http://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python

    # something to write a file

    # stream.filter(track=["string1", "string2"])
    # filters to track by string1, string2. can enter a longer list as well.
    # stop program with Ctrl-C
    # to run, from command line: python file_name.py > twitter_data.text
    # here file_name is the name of the file containing this listener
