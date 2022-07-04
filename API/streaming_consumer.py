from email.header import decode_header
from kafka import KafkaConsumer
from time import sleep
import os
from pyspark.sql import SparkSession
import pyspark
import multiprocessing
from pyspark.sql.types import StringType,FloatType,IntegerType,StructType,StructField,ArrayType,BooleanType

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 pyspark-shell'
# specify the topic we want to stream data from.
kafka_topic_name = "PinterestTopic"
# Specify your Kafka server to read data from.
kafka_bootstrap_servers = 'localhost:9092'

#pyspark config
cfg = (
    pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("pinterestpipeline_streaming")
    .set("spark.eventLog.enabled", False)
    .set("spark.executor.memory", "1g")
)

#creating sparksession
spark = SparkSession \
        .builder \
        .appName("KafkaStreaming")\
        .config(conf=cfg)\
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("group_id","group1") \
        .option("startingOffsets", "earliest") \
        .load()

#function to fix is_image_or_video column
def fix_is_image_or_video(x) -> StringType():
    if x == 'multi-video(story page format)':
        return 'video'
    elif x == 'image':
        return 'image'
    else:
        return StringType(x)

#function change follower count to integer
def fix_follower_count(x) -> float:
    if type(x) == float or type(x) == int:
        return FloatType(x)
    if 'k' in x:
        if len(x) > 1:
            return FloatType(x.replace('k', '')) * 1000
        return 1000.0
    if 'm' in x:
        if len(x) > 1:
            return FloatType(x.replace('m', '')) * 1000000
        return 1000000.0

#function to fix tag list
def fix_tag_list(tag_list) -> ArrayType(StringType()):
    if tag_list is None:
        return None
    else:
        return ArrayType(StringType(tag_list.split(',')))

# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")

# outputting the messages to the console 
stream_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()\
    .awaitTermination()
