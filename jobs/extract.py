import os
import time

from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    regexp_replace,
    split,
)

from jobs.read_data import read_csv_data
from jobs.write import write_parquet

path = os.getenv("SPARK_DATA_PATH", default="./data/listings")
path_output = os.getenv("SPARK_OUTPUT_PATH", default="./output/extracted_data/listing")


def extract_listing_data():
    df = read_csv_data(path)

    extract_selected_column = (
        df.select(COLUMNS)
        .withColumn("city", regexp_extract(input_file_name(), "\\/([^/]*)$", 1))
        .withColumn("city", split(col("city"), "_").getItem(1))
        .withColumn("price", regexp_replace(col("price"), "[$|,]", "").cast("double"))
    )
    write_parquet(extract_selected_column, path_output, partition_by="city")

    time.sleep(10000)


def extract_review_data():
    df = read_csv_data("./data/reviews")

    extracted_review_data = (
        df.select("listing_id", "id")
        .withColumn("city", regexp_extract(input_file_name(), "\\/([^/]*)$", 1))
        .withColumn("city", split(col("city"), "_").getItem(1))
        .withColumnRenamed("id", "review_id")
    )
    write_parquet(
        extracted_review_data, "./output/extracted_data/reviews", partition_by="city"
    )


COLUMNS = [
    "id",
    "price",
    "accommodates",
    "availability_365",
    "host_id",
    "host_acceptance_rate",
    "host_response_rate",
    "host_response_time",
    "review_scores_rating",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "number_of_reviews_ly",
]

if __name__ == "__main__":
    extract_listing_data()
