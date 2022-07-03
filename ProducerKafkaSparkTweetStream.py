# -*- coding: utf-8 -*-
import tweepy
from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
from pyspark.sql import functions as f
from datetime import datetime
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import time
from datetime import datetime
import json
import pymongo
import os

"""## Base de données SQL"""

client = pymongo.MongoClient(
    "mongodb+srv://twitterbot:SaPDBU5WU3O349mn@cluster0.z5t7h.mongodb.net/?retryWrites=true&w=majority"
)
twitteranalysis_database = client.twitteranalysis

"""#  Configuration de Twitter 

getting the API object using authorization information
"""

# twitter setup
consumer_key = "886bWUB38AHD1VC8vE777rVKs"
consumer_secret = "QLTWRcxbmxjOAAJatf4WCbL7j5vQYiyhSImv00wLarPVctcXE4"
access_token = "765095367067262976-Nz1XFSRSQjdd2MKYdPLiyKpjTUSsEoo"
access_token_secret = "8GZJcQVFJEk4SBrGgjk0V1HXiFau920Jt62Dntgf65qug"
bearer_token = "AAAAAAAAAAAAAAAAAAAAAF0CeAEAAAAAep52nhowJZuk9B5RzizY8pJ7UfU%3Da5MK3zCKaEvktiqRgFrwwIt3gqkU53nNYWgnkl9bXbf2ygfscy"
# Creating the authentication object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
# Setting your access token and secret
auth.set_access_token(access_token, access_token_secret)
# Creating the API object by passing in auth information
api = tweepy.API(auth)

#try connection
try:
    api.verify_credentials()
    print("Authentication Successful")
except:
    print("Authentication Error")

"""# Création du producer Kafka 

- specify the Kafka Broker
- specify the topic name
- optional: specify partitioning strategy
"""

topic_name = 'tweets_stream'

def kafka_stream_tweets(data):
  producer = KafkaProducer(
    value_serializer=lambda m: json.dumps(m).encode('utf-8'),
    bootstrap_servers='localhost:9092'
  )
  print(data)
  producer.send(topic_name, value=data)
        
  return True

class Streamer(StreamingClient):
    def on_data(self, data):
        json_data = json.loads(data)
        stream_messages = {}
        print(f"Preparing message ")
        event_time = datetime.now()
        stream_messages["tweet"] = json_data["data"]
        stream_messages["date_time"] = event_time.strftime("%Y-%m-%d %H:%M:%S")
        print(stream_messages)
        print(f"Kafka messages: {stream_messages}")
        kafka_stream_tweets(stream_messages)
        time.sleep(1)
        
        return True

    def on_connection_error(self):
        self.disconnect()
    
    def on_status(self, status):
        print(status.id)


KafkaPrinter = Streamer(bearer_token)
rule = StreamRule(value="harcelement")
KafkaPrinter.add_rules(rule)
KafkaPrinter.filter(tweet_fields="created_at")
KafkaPrinter.filter()