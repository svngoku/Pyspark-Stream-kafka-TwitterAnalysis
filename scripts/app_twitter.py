# Write a pyspark app, spark streaming and kafka who get tweets in real-time with the Twitter API
# Store datas each 4s and save everything in a postgres database

# pip install tweepy
# pip install sqlalchemy
# pip install psycopg2

import sys
import time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

from datetime import datetime

import sqlalchemy

#Initialization Spark
sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 4)

#listener class to consume the stream
class StdOutListener(StreamLstener):
    def on_data(self, data):
        try:
            # loads json data
            decoded = json.loads(data)
            #if it's a tweet
            if decoded.get('user'):
                user = decoded['user']['screen_name']
                user_id = decoded['user']['id']
                tweet_id = decoded['id']
                tweet = decoded['text']
                created_at = decoded['created_at']
                created_at = created_at.replace("+0000 ","")
                created_at = datetime.strptime(created_at,"%a %b %d %H:%M:%S %Y")
                lang = decoded['lang']
                retweet_count = decoded['retweet_count']
                favorites_count = decoded['favorite_count']

                #print datas of the tweet
                print("Screen Name: ",user)
                print("User_id: ",user_id)
                print("Tweet_id: ",tweet_id)
                print("Tweet: ",tweet)
                print("Created_at: ",created_at)
                print("Lang: ",lang)
                print("Retweet_count: ",retweet_count)
                print("Favorites_count: ",favorites_count)
                print("\n")

                #insert datas in database
                engine = sqlalchemy.create_engine('postgresql://db_user:password@localhost:5432/twitter_db')
                connection = engine.connect()
                connection.execute("INSERT INTO twitter_tweets (user_id, user, tweet_id, tweet, created_at, lang, retweet_count, favorites_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (user_id, user, tweet_id, tweet, created_at, lang, retweet_count, favorites_count))

                return True
        except Exception as e:
            print (e)
            return True

    #error
    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    #twitter API keys
    consumer_key = "XXXX"
    consumer_secret = "XXXX"
    access_token = "XXXX"
    access_token_secret = "XXXX"

    #connection with twitter API
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    #streaming with the listener class
    stream = Stream(auth, StdOutListener())

    #specify the topic
    stream.filter(track=['spark'])

    ssc.start()
    ssc.awaitTermination()
