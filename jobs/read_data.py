from pyspark.sql import DataFrame

from config.spark_config import spark

def read_csv_data(path : str, sr: float = 0.1) -> DataFrame :
    return spark.read.csv(path, header=True, inferSchema=True, quote='"', escape='"', multiLine=True, samplingRatio=sr)

def read_parquet_data(path: str) -> DataFrame :
    return spark.read.parquet(path)