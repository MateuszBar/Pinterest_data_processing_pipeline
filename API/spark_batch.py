import multiprocessing
import pyspark
import operator
import boto3 
import json
import configparser
import findspark
import os
from pyspark.sql.functions import col,lit,split
from pyspark import SparkContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import StringType,FloatType,IntegerType,StructType,StructField,ArrayType,BooleanType
from cassandra.cluster import Cluster


#setting up s3 bucket
s3 = boto3.resource('s3')
# get a handle on the bucket that holds your file
bucket = s3.Bucket('pinterestprojectpipeline')
# get a handle on the object you want (i.e. your file)
obj = bucket.Object(key='PinterestData1.json')
# get the object
response = obj.get()

findspark.init()

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get("default", "aws_access_key_id") 
access_key = config.get("default", "aws_secret_access_key")

# config for pyspark session
cfg = (
    pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("pinterestpipeline_batchprocessing")
    .set("spark.eventLog.enabled", False)
    .set("spark.executor.memory", "1g")
)

#creating session
session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()
sc = session.sparkContext

df = session.createDataFrame(json.loads(response['Body'].read().decode('utf-8')))

#function to change normalise is_image_or_video column
def fix_is_image_or_video(x) -> StringType():
    if x == 'multi-video(story page format)':
        return 'video'
    elif x == 'image':
        return 'image'
    else:
        return x
#create udf for is_image_or_video function
udf_img_or_vid = UserDefinedFunction(lambda x:fix_is_image_or_video(x), StringType())

#function change follower count to integer
def fix_follower_count(x) -> float:
    if type(x) == float or type(x) == int:
        return x
    if 'k' in x:
        if len(x) > 1:
            return float(x.replace('k', '')) * 1000
        return 1000.0
    if 'm' in x:
        if len(x) > 1:
            return float(x.replace('m', '')) * 1000000
        return 1000000.0

udf_fix_follower_count = UserDefinedFunction(lambda x:fix_follower_count(x), FloatType())

udf_fix_tag_list = UserDefinedFunction(lambda x:x.split(','), ArrayType(StringType()))

#apply functions to dataframe
new_df = df.withColumn("is_image_or_video", udf_img_or_vid(col("is_image_or_video")))
new_df = new_df.withColumn("follower_count", udf_fix_follower_count(col("follower_count")))
new_df = new_df.withColumn("tag_list", udf_fix_tag_list(col("tag_list")))

new_df.coalesce(1).write.mode('overwrite').format('json').save('temp_pinterestdata')