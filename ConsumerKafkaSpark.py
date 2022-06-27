from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from models.utils import cleanTweet, Subjectivity, Polarity, Sentiment, writeIntoMongo, define_tweet_schema

topic_name = 'tweets_stream'
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    spark = SparkSession.builder \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
            .appName("TwitterAnalysisApp") \
            .getOrCreate()
    # CHECK SPARK CONTEXT
    spark.sparkContext.setLogLevel("ERROR")

    # READ STREAM DATAS FROM THE PRODUCER
    tweet_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    tweet_df  = tweet_df.selectExpr("CAST(value AS STRING)")

    tweet_schema = define_tweet_schema()

    tweet_df2 = tweet_df \
        .select(from_json(col("value"), tweet_schema)\
        .alias("tweets"))

    tweet_df2.printSchema()
    # print(tweet_df2.columns)
    tweet_df3 = tweet_df2.select("tweets.*")

    clean_tweets_udf = udf(cleanTweet, StringType())

    cl_tweets = tweet_df3.withColumn('processed_text', clean_tweets_udf(col("tweet")))
    subjectivity_tw = cl_tweets.withColumn('subjectivity', Subjectivity(col("processed_text")))
    polarity_tw = subjectivity_tw.withColumn("polarity", Polarity(col("processed_text")))
    sentiment_tw = polarity_tw.withColumn("sentiment", Sentiment(col("polarity")))

    #df = tweet_df3.select("tweets.*",tweet_schema)

    sentiment_tw.printSchema()

    # Ecrire en console
    # write_tweet = sentiment_tw \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()

    #    write_tweet = orders_df1.apply(udf(getSentiment(),StringType())).writeStream.foreach(writeIntoMongo()).start()

    write_tweet = sentiment_tw.writeStream.foreach(writeIntoMongo()).start()
    write_tweet.awaitTermination()


