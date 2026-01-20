import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    split,
)
from pyspark.sql.types import DateType, DoubleType, LongType
from pyspark.sql.window import Window

from jobs.read_data import read_csv_data
from jobs.write import write_parquet

path = os.getenv("SPARK_DATA_PATH", default="./data/listings")
path_output = os.getenv("SPARK_OUTPUT_PATH", default="./output/extracted_data/listing")


def convert_price_to_usd(df: DataFrame) -> DataFrame:
    conversion_rates = {
        "new-york-city": 1.0,
        "los-angeles": 1.0,
        "san-francisco": 1.0,
        "chicago": 1.0,
        "austin": 1.0,
        "london": 1.347,
        "paris": 1.166,
        "barcelona": 1.166,
        "amsterdam": 1.166,
        "berlin": 1.166,
        "tokyo": 0.0063,
        "sydney": 0.671,
        "bangkok": 0.032,
        "toronto": 0.721,
        "mexico-city": 0.056,
    }

    c = F.when(F.col("city") == "", 1.0)
    for city, rate in conversion_rates.items():
        c = c.when(F.col("city") == city, rate)
    c.otherwise(1.0)

    return df.withColumn("price", c * F.col("price"))



def extract_listing_data():
    df = read_csv_data(path)
    extract_selected_column = (
        df.select(COLUMNS)
        .dropna()
        .withColumn("city", regexp_extract(input_file_name(), "\\/([^/]*)$", 1))
        .withColumn("city", split("city", "_").getItem(1))
        .transform(convert_price_to_usd)
    )


    latest_with_price = (
        extract_selected_column.withColumn(
            "rn", F.row_number().over(Window.partitionBy("id").orderBy(F.col("last_scraped").desc()))
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    ).cache()

    write_parquet(latest_with_price, path_output, partition_by="city")

    latest_with_price.printSchema()
    print(latest_with_price.count())


def extract_review_data():
    df = read_csv_data("./data/reviews")

    extracted_review_data = (
        df.select("listing_id", "id")
        .withColumn("city", regexp_extract(input_file_name(), "\\/([^/]*)$", 1))
        .withColumn("city", split(col("city"), "_").getItem(1))
        .withColumnRenamed("id", "review_id")
    )
    write_parquet(extracted_review_data, "./output/extracted_data/reviews", partition_by="city")


COLUMNS = [
    col("id").cast(LongType()),
    col("last_scraped").cast(DateType()),
    F.try_to_number("price", F.lit("$999,999,999.99")).cast(DoubleType()).alias("price"),
    col("accommodates").try_cast(DoubleType()),
    col("estimated_occupancy_l365d").try_cast(DoubleType()).alias("occupancy"),
    col("host_id").cast(LongType()),
    F.replace(col("host_acceptance_rate"), F.lit("%")).try_cast(DoubleType()).alias("host_acceptance_rate"),
    F.replace(col("host_response_rate"), F.lit("%")).try_cast(DoubleType()).alias("host_response_rate"),
    col("host_response_time"),
    col("review_scores_rating").try_cast(DoubleType()),
    col("review_scores_accuracy").try_cast(DoubleType()),
    col("review_scores_cleanliness").try_cast(DoubleType()),
    col("review_scores_checkin").try_cast(DoubleType()),
    col("review_scores_communication").try_cast(DoubleType()),
    col("review_scores_location").try_cast(DoubleType()),
    col("review_scores_value").try_cast(DoubleType()),
    col("number_of_reviews").try_cast(LongType()),
    col("number_of_reviews_ltm").try_cast(LongType()),
    col("number_of_reviews_l30d").try_cast(LongType()),
    col("number_of_reviews_ly").try_cast(LongType()),
]

if __name__ == "__main__":
    extract_listing_data()
