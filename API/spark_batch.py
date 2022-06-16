import multiprocessing
import pyspark
import operator
import boto3 
import json
import configparser
import os
from pyspark import SparkContext

config = configparser.ConfigParser()
config.read(os.path.expanduser("~/.aws/credentials"))
access_id = config.get("default", "aws_access_key_id") 
access_key = config.get("default", "aws_secret_access_key")

# config or pyspark session
cfg = (
    pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("pinterestpipeline_batchprocessing")
    .set("spark.executor.memory", "1g")
)

session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

hadoop_conf=session._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")
hadoop_conf.set("fs.s3.awsAccessKeyId", access_id)
hadoop_conf.set("fs.s3.awsSecretAccessKey", access_key)
hadoop_conf.set("fs.s3.endpoint", "s3.amazonaws.com")

df=session.SparkContext.read.json("s3://pinterestprojectpipeline/PinterestData*.json")
df.show()