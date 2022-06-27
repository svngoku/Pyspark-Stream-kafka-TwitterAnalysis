import re
from dataclasses import dataclass
from pyspark.sql import SparkSession, DataFrame, functions as f
from pyspark.sql.types import Row, StructType, StructField, TimestampType, StringType, IntegerType, ArrayType, DataType, FloatType
from pyspark.sql.functions import udf
from pyspark.streaming import StreamingContext
import json
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
from datetime import datetime
from pyspark.sql.functions import udf, col
from textblob import TextBlob
from pymongo import MongoClient


class writeIntoMongo:
    def open(self,partition_id,epcoh_id):
        self.client = MongoClient(
            "mongodb+srv://twitterbot:SaPDBU5WU3O349mn@cluster0.z5t7h.mongodb.net/?retryWrites=true&w=majority"
        )
        self.database = self.client["twitteranalysis"]
        self.collection = self.db["twitter_stream"]
        return 1
    
    def process(self,row):
        self.collection.insert_one(row.asDict())
    
    def close(self,error):
        self.client.close()
        return 1

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


def define_tweet_schema():
    orders_schema = StructType([StructField("tweet", StringType(), True) \
        ,StructField("tweet_datetime", StringType(), True) \
            , StructField("processed_text", StringType(), True) \
                , StructField("subjectivity", FloatType(), True) \
                    , StructField("polarity", FloatType(), True) \
                        , StructField("sentiment", StringType(), True)])

    return orders_schema

def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # Remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # Remove punctuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # Remove numbers
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # Remove hashtags
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # Remove extra simbols
    tweet = re.sub('@\w+', '', str(tweet))
    tweet = re.sub('\n', '', str(tweet))

    # Remove tweets
    emoji_pattern = re.compile(
            "["u"\U0001F600-\U0001F64F", 
            "\U0001F300-\U0001F5FF",
            u"\U0001F680-\U0001F6FF",
            u"\U0001F1E0-\U0001F1FF",
            u"\U00002702-\U000027B0",
            u"\U000024C2-\U0001F251""]+", flags=re.UNICODE)
    
    # Apply the remove emoji pattern
    tweet = emoji_pattern.sub(r'', tweet)    
    return tweet


@udf(returnType=StringType())
def Subjectivity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.subjectivity

@udf(returnType=FloatType())
def Polarity(tweet: str) -> float:
    return TextBlob(tweet).sentiment.polarity

@udf(returnType=StringType())
def Sentiment(polarityValue: int) -> str:
    if polarityValue < 0:
        return 'Negative'
    elif polarityValue == 0:
        return 'Neutral'
    else:
        return 'Positive'
