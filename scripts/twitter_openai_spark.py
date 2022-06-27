# Write a pyspark application connected with kafka  to get tweets in real-time and store it inside Kafka topic base in the tag and use udf for building transformation functions to parse tweet datas
# use tweepy, json, pyspark, kafka and dataclass
from typing import List
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.types import Row, StructType, StructField, TimestampType, StringType, IntegerType
from pyspark.sql.functions import udf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from tweepy import OAuthHandler, AppAuthHandler
from tweepy.streaming import StreamListener
from tweepy import Stream
import json
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from datetime import datetime


# Tweepy function class for twitter streaming
class MyStreamListener(StreamListener):
    def __init__(self, spark_session: SparkSession, topic: str, debug: bool = False):
        """
        Initialize the stream listener.
        :param spark_session: Spark session to write data to.
        :param topic: Kafka topic to write data to.
        :param debug: If True, print all messages to stdout.
        """
        self.spark_session = spark_session
        self.topic = topic
        self.debug = debug
        self.output = []

    # on_data will be called when the tweet stream is running. 
    # Function will be called once for each status received.
    def on_data(self, data):
        # if the stream listener is in debug mode print the tweet
        if self.debug:
            print(data)
        # convert from json to dictionary
        data_dict = json.loads(data)
        # check if there is any extended_tweet which contains more than 140 characters
        if "extended_tweet" in data_dict:
            tweet = data_dict["extended_tweet"]["full_text"]
        else:
            tweet = data_dict["text"]
        # get the user information
        user_name = data_dict["user"]["name"]
        user_screen_name = data_dict["user"]["screen_name"]
        user_location = data_dict["user"]["location"]
        # build the tweet structure
        tweet_datas = Tweet_datas(user_name, user_screen_name, user_location, tweet, data_dict["created_at"])
        # add the tweet
        # convert the tweet to json and store it as string
        self.output.append(json.dumps(tweet_datas.__dict__))
        # create the dataframe
        df = self.spark_session.read.json(self.output)
        # self.output = []
        # only keep the important features
        df = df.select(df.created_at, df.text, df.user_name, df.user_screen_name, df.user_location)
        # explode the dataframe
        df = df.withColumn("raw_tags", f.explode(f.split(df.text, "\s"))).select("raw_tags", "user_name", "created_at", "user_location", "user_screen_name")
        # add the namespace
        df = df.withColumn("namespace", f.lit("tweets"))
        # build the tag function
        add_tag = udf(lambda text: " " + text if text[0] == "#" else None)
        # now apply the tag
        df = df.withColumn("tag", add_tag("raw_tags"))
        # drop raw_tags
        df = df.drop("raw_tags")
        # fill the blank place with null
        df = df.fillna("null", subset=["tag"])
        # filter the null value
        df = df.filter(df.tag != "null")
        # save into Kafka
        df.show(truncate=False)
        df.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", self.topic) \
            .save()
        # write data to kafka
        # df.writeStream \
        #     .format("kafka") \
        #     .option("kafka.bootstrap.servers", "localhost:9092") \
        #     .option("topic", self.topic) \
        #     .start()
        return True

    # Function is called when an error occurs
    def on_error(self, status_code):
        print("error code:", status_code)
        return True
    
    # Function to handle being disconnected from the streaming api
    def on_disconnect(self, notice):
        print("disconnect notice:", notice)
        return True

# data class for tweet entity
@dataclass
class Tweet_datas:
    """
    A dataclass to represent the data extracted from a tweet.
    """
    user_name: str
    user_screen_name: str
    user_location: str
    text: str
    created_at: str
    
    def __str__(self):
        return f"""user_name: {self.user_name},
                                user_screen_name: {self.user_screen_name},
                                user_location: {self.user_location},
                                text: {self.text},
                                date_time: {self.created_at}"""

# decision maker
def main():
    tags = ["#Apple", "#Tesla", "#Microsoft"]
    consumer_key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    consumer_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    access_token = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    access_token_secret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("TwitterApp") \
        .getOrCreate()

    # Create Streaming Context
    sc = StreamingContext(spark.sparkContext, 5)
    
    # Create Twitter Context
    myStream = Stream(auth=OAuthHandler(consumer_key, consumer_secret), listener=MyStreamListener(spark_session=spark, topic="tweets"))
    # if there's no tags to use
    if not tags:
        print("You must provide at least one tag to search for.")
        sys.exit(0)
    # check if the tags list is bigger than 0
    myStream.filter(track=tags)
    # Use the DataFrame as stream
    kafka_stream = KafkaUtils.createDirectStream(sc, "tweets")
    # get the value from kafka
    df_kafka_stream = kafka_stream.map(lambda x: json.loads(x[1])).toDF()
    print(f"This is the stream datas: {df_kafka_stream}")
    # explode the dataframe
    df_stream = df_kafka_stream.select(df_kafka_stream.text, df_kafka_stream.created_at, "user_name", "user_screen_name", "user_location") \
        .withColumn("tweet", f.explode(f.split(df_kafka_stream.text, "\s"))).select("tweet", "created_at", "user_name", "user_screen_name", "user_location")
    df_stream.show(6, truncate=False)
    print(f"This is the stream dataframe: {df_stream}")
    # write data to kafka
    df_stream.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("topic", "tweets") \
        .start()
    # stream the frame
    df_stream.stream.foreach(process_new_data)
    # Start the Streaming Context
    sc.start()
    # Wait for the Streaming Context to `stop()` or terminate with error
    sc.awaitTermination()

# process_new_data function to append data to dataframe in streaming
def process_new_data(new_data):
    print(new_data)


if __name__ == "__main__":
    main()
