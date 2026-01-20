import time

import plotly.express as px
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from jobs.read_data import read_parquet_data


def clean_percentage_col(df, col_name):
    return (
        df.withColumn(col_name, F.regexp_replace(F.col(col_name), "%", ""))
        .withColumn(col_name, F.regexp_replace(F.col(col_name), "N/A", "0"))
        .withColumn(col_name, F.col(col_name).cast("int"))
    )


def clean_response_time_col(df, col_name):
    mapping = {
        "within an hour": "1",
        "a few days or more": "48",
        "within a day": "24",
        "within a few hours": "6",
        "N/A": "72",
    }
    return df.replace(mapping, subset=[col_name]).withColumn(
        col_name, F.col(col_name).cast("int")
    )


def calculate_host_ecosystem_across_cities(listing_data: DataFrame) -> DataFrame:
    df = clean_percentage_col(listing_data, "host_acceptance_rate")
    df = clean_percentage_col(df, "host_response_rate")
    df = clean_response_time_col(df, "host_response_time")
    return df.groupby("city").agg(
        F.mean("host_acceptance_rate").alias("avg_host_acceptance_rate"),
        F.mean("host_response_rate").alias("avg_host_response_rate"),
        F.mean("host_response_time").alias("avg_host_response_time"),
    )


if __name__ == "__main__":
    df = read_parquet_data("./output/extracted_data/listing").fillna("0")
    selected_columns = calculate_host_ecosystem_across_cities(df)
    px.line(
        selected_columns,
        "city",
        ["avg_host_acceptance_rate", "avg_host_response_rate"],
        title="Host Accept",
    ).update_layout(yaxis_title="Percentage").write_image(
        "./output/images/hosts/host_acceptance_and_response.png"
    )
    px.line(
        selected_columns,
        "city",
        ["avg_host_response_time"],
        title="Host Response time",
    ).update_layout(yaxis_title="Hours").write_image(
        "./output/images/hosts/host_response_time.png"
    )
