import os

from pyspark.sql import SparkSession

master_url = os.getenv("SPARK_MASTER_URL", default="local[*]")

spark = (
    SparkSession.builder.appName("Airbnb Listing Overview")
    .config("spark.driver.memory", "2G")
    .master(master_url)
    .getOrCreate()
)

spark.conf.set("spark.sql.adaptive.enabled", "true")

spark.sparkContext._jsc.hadoopConfiguration().set("fs.file.impl.disable.cache", "true")
