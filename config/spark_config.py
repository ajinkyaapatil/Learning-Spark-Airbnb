from pyspark.sql import SparkSession
import os

master_url = os.getenv('SPARK_MASTER_URL', default='local[*]')

spark = SparkSession.builder \
    .appName("Airbnb Listing Overview") \
    .master(master_url) \
    .getOrCreate()

spark.sparkContext._jsc.hadoopConfiguration().set("fs.file.impl.disable.cache", "true")
