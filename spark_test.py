import multiprocessing
import pyspark
import operator

# config or pyspark session
cfg = (
    pyspark.SparkConf()
    .setMaster(f"local[{multiprocessing.cpu_count()}]")
    .setAppName("testapp")
    .set("spark.executor.memory", "1g")
)

# Get or create a SparkSession
session = pyspark.sql.SparkSession.builder.config(conf=cfg).getOrCreate()

# spark context object
sc = session.sparkContext

data = list(range(10,-11,-1))
print(data)

result = (
    sc.parallelize(data)
    .filter(lambda val: val % 3 == 0)
    .map(operator.abs)
    .fold(0,operator.add)
)

print(result)