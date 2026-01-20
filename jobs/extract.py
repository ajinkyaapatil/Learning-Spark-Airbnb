import os

import pyspark.sql.functions as F
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


def extract_listing_data():
    df = read_csv_data(path)

    extract_selected_column = (
        df.select(COLUMNS)
        .withColumn("city", regexp_extract(input_file_name(), "\\/([^/]*)$", 1))
        .withColumn("city", split("city", "_").getItem(1))
        .dropna()
        # .filter(col("price").isNotNull())
    )

    latest_with_price = (
        extract_selected_column.withColumn(
            "rn", F.row_number().over(Window.partitionBy("id").orderBy(F.col("last_scraped").desc()))
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
    )
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
    F.try_to_number("price", F.lit("$999,999,999.99")).cast("double"),
    col("accommodates").try_cast(DoubleType()),
    col("estimated_occupancy_l365d").try_cast(DoubleType()).alias("occupancy"),
    col("host_id").cast(LongType()),
    F.replace(col("host_acceptance_rate"), F.lit("%")).try_cast(DoubleType()).alias("host_acceptance_rate"),
    F.replace(col("host_response_rate"), F.lit("%")).try_cast(DoubleType()).alias("host_response_rate"),
    col("host_response_time"),
    # col("review_scores_rating").cast(DoubleType()),
    # col("review_scores_accuracy").cast(DoubleType()),
    # col("review_scores_cleanliness").cast(DoubleType()),
    # col("review_scores_checkin").cast(DoubleType()),
    # col("review_scores_communication").cast(DoubleType()),
    # col("review_scores_location").cast(DoubleType()),
    # col("review_scores_value").cast(DoubleType()),
    # col("number_of_reviews").cast(LongType()),
    # col("number_of_reviews_ltm").cast(LongType()),
    # col("number_of_reviews_l30d").cast(LongType()),
    # col("number_of_reviews_ly").cast(LongType()),
]

if __name__ == "__main__":
    extract_listing_data()
