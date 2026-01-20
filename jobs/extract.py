import os

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DateType, DoubleType, LongType
from pyspark.sql.window import Window

from jobs.read_data import read_csv_data
from jobs.write import write_parquet

path = os.getenv("SPARK_DATA_PATH", default="./data/listings")
path_output = os.getenv("SPARK_OUTPUT_PATH", default="./output/extracted_data/listing")

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


def convert_price_to_usd(df: DataFrame) -> DataFrame:
    c = F.when(F.col("city") == "", 1.0)
    for city, rate in conversion_rates.items():
        c = c.when(F.col("city") == city, rate)
    c.otherwise(1.0)

    return df.withColumn("price", c * F.col("price"))


def get_latest_price(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("rn", F.row_number().over(Window.partitionBy("id").orderBy(F.col("last_scraped").desc())))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )


def add_city(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "city",
        F.split(
            F.regexp_extract(F.input_file_name(), "\\/([^/]*)$", 1),
            "_",
        ).getItem(1),
    )


def extract_listing_data():
    df = (
        read_csv_data(path)
        .select(COLUMNS)
        .dropna()
        .transform(add_city)
        .localCheckpoint()
        .transform(get_latest_price)
        .transform(convert_price_to_usd)
    )

    write_parquet(df, path_output)

    df.printSchema()
    print(df.count())


def extract_review_data():
    df = read_csv_data("./data/reviews")

    extracted_review_data = (
        df.select("listing_id", "id")
        .withColumn("city", F.regexp_extract(F.input_file_name(), "\\/([^/]*)$", 1))
        .withColumn("city", F.split(F.col("city"), "_").getItem(1))
        .withColumnRenamed("id", "review_id")
    )
    write_parquet(extracted_review_data, "./output/extracted_data/reviews", partition_by="city")


COLUMNS = [
    F.col("id").cast(LongType()),
    F.col("last_scraped").cast(DateType()),
    F.try_to_number("price", F.lit("$999,999,999.99")).cast(DoubleType()).alias("price"),
    F.col("accommodates").try_cast(DoubleType()),
    F.col("estimated_occupancy_l365d").try_cast(DoubleType()).alias("occupancy"),
    F.col("host_id").cast(LongType()),
    F.replace(F.col("host_acceptance_rate"), F.lit("%")).try_cast(DoubleType()).alias("host_acceptance_rate"),
    F.replace(F.col("host_response_rate"), F.lit("%")).try_cast(DoubleType()).alias("host_response_rate"),
    F.col("host_response_time"),
    F.col("review_scores_rating").try_cast(DoubleType()),
    F.col("review_scores_accuracy").try_cast(DoubleType()),
    F.col("review_scores_cleanliness").try_cast(DoubleType()),
    F.col("review_scores_checkin").try_cast(DoubleType()),
    F.col("review_scores_communication").try_cast(DoubleType()),
    F.col("review_scores_location").try_cast(DoubleType()),
    F.col("review_scores_value").try_cast(DoubleType()),
    F.col("number_of_reviews").try_cast(LongType()),
    F.col("number_of_reviews_ltm").try_cast(LongType()),
    F.col("number_of_reviews_l30d").try_cast(LongType()),
    F.col("number_of_reviews_ly").try_cast(LongType()),
]

if __name__ == "__main__":
    extract_listing_data()
