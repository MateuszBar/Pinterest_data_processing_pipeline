from email.header import decode_header
from kafka import KafkaConsumer
from time import sleep
import os
from pyspark.sql import SparkSession
import pyspark
import multiprocessing
from pyspark.sql.types import StringType,FloatType,IntegerType,StructType,StructField,ArrayType,BooleanType,NullType
from pyspark.sql.functions import col,UserDefinedFunction,expr
from passwords import postgres_user,postgres_password

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1,org.postgresql:postgresql:42.4.0 pyspark-shell'

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
    '''
    Filters input into two types of output

        Args: x - input string

        Returns: 'video' or 'image' or x - string type 
    '''
    if x == 'multi-video(story page format)':
        return 'video'
    elif x == 'image':
        return 'image'
    else:
        return x

#function to change follower count to float
def fix_follower_count(x) -> float:
    '''
    Function that replaces k with 1000 and changes string into float

        Args: x - input string

        Returns: x converted into float type
    '''
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

#function to fix errors in image_src column
def fix_image_src(x) -> StringType():
    '''
    Function that changes image src error data into null values

        Args: x - string

        Returns: Null Value
    '''
    if x == "Image src error.":
        return NullType()
    else:
        return x

#defining UDFs into memory
fix_is_image_or_video_UDF = UserDefinedFunction(lambda x: fix_is_image_or_video(x), StringType())
fix_follower_count_UDF = UserDefinedFunction(lambda x: fix_follower_count(x), FloatType())
fix_image_src_UDF = UserDefinedFunction(lambda x: fix_image_src(x), StringType())

#apply mappings to df stream
stream_df.replace("multi-video(story page format)", "video")
stream_df = stream_df.withColumn("is_image_or_video", expr('fix_is_image_or_video_UDF(is_image_or_video)'))
stream_df = stream_df.withColumn("image_src", expr('fix_image_src_UDF(image_src)'))
# Select the value part of the kafka message and cast it to a string.
stream_df = stream_df.selectExpr("CAST(value as STRING)")

# outputting the messages to the console 
#stream_df.writeStream \
#    .format("jdbc") \
#    .outputMode("append") \
#    .start()\
#    .awaitTermination()

# function to send data to postgres database
def _write_streaming(
    df,
    epoch_id
) -> None:         

    df.write \
        .mode('append') \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "PinterestProject") \
        .option("user", postgres_user) \
        .option("password", postgres_password) \
        .save() 

# start writing stream
stream_df.writeStream \
    .foreachBatch(_write_streaming) \
    .start()\
    .awaitTermination()
