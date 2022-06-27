import os
import time
import json
import psycopg2
import tweepy
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils

# consumer key, consumer secret, access token, access secret.
ckey = "YOUR KEY"
csecret = "YOUR SECRET"
atoken = "YOUR TOKEN"
asecret = "YOUR SECRET"

auth = tweepy.OAuthHandler(ckey, csecret)
auth.set_access_token(token, asecret)

api = tweepy.API(auth)

# Postgres store
conn = psycopg2.connect(
    host="localhost",database="twitter", 
    user="postgres", password="postgres"
)

def saveToPostgres(rdd):
    # Parse rdd in json
    tweets = rdd.map(lambda x: json.loads(x[1]))
    # Print number of tweets in the rdd
    print("Number of tweets : " + str(tweets.count()))

    tweets.foreach(lambda x: saveToDB(x))

def saveToDB(x):
    # Try to save the tweet in the database
    try:
        cur = conn.cursor()
        cur.execute("INSERT INTO tweets VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",(x['id'], x['created_at'], x['text'], \
        x['user']['id'], x['user']['name'], x['user']['screen_name'], x['place']['id'], x['place']['name'], x['place']['bounding_box']['coordinates'][0][0]))
        conn.commit()
        cur.close()
        print("Saved !")
    except Exception as e:
        print("Error : " + str(e))

def getTweets():
    # Get tweets
    try:
        # Get tweets from Paris (geocode) and with the keywords "Trump" and "election"
        tweets = api.search(q = ["Trump", "elections"], geocode = "48.864716,2.349014,10km", lang = "en", count = 100)
        # Return tweets
        return tweets
    except Exception as e:
        print("Error : " + str(e))

def sendTweets(rdd):
    # Get tweets and send to Kafka
    try:
        # Send tweets
        for tweet in getTweets():
            rdd.context.socketTextStream("localhost", 9999).send(json.dumps(tweet._json))
    except Exception as e:
        print("Error : " + str(e))

if __name__ == "__main__":
    sc = SparkContext(appName="TwitterStreaming")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, 4)

    # Kafka topic
    kstream = KafkaUtils.createDirectStream(ssc, ["twitter"], {"metadata.broker.list": "localhost:9092"})

    # Kafka consumer
    kstream.foreachRDD(lambda rdd: saveToPostgres(rdd))

    # Start Twitter stream
    getTweets()
    ssc.start()
    ssc.awaitTermination()
