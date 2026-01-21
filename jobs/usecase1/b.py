import os

import plotly.express as px
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType

from jobs.read_data import read_parquet_data

response_time_to_hours = {
    "within an hour": "1",
    "within a few hours": "6",
    "within a day": "24",
    "a few days or more": "48",
    "N/A": "72",
}


def convert_response_time_in_hours(dataframe):
    return dataframe.replace(response_time_to_hours, subset=["host_response_time"]).withColumn(
        "host_response_time", F.col("host_response_time").cast(IntegerType())
    )


def calculate_host_ecosystem_across_cities(listing_data: DataFrame) -> DataFrame:
    return (
        listing_data.transform(convert_response_time_in_hours)
        .groupby("city")
        .agg(
            F.mean("host_acceptance_rate").alias("avg_host_acceptance_rate"),
            F.mean("host_response_rate").alias("avg_host_response_rate"),
            F.mean("host_response_time").alias("avg_host_response_time"),
            F.mean("review_scores_rating").alias("avg_review_scores_rating"),
            F.mean("review_scores_accuracy").alias("avg_review_scores_accuracy"),
            F.mean("review_scores_cleanliness").alias("avg_review_scores_cleanliness"),
            F.mean("review_scores_checkin").alias("avg_review_scores_checkin"),
            F.mean("review_scores_communication").alias("avg_review_scores_communication"),
            F.mean("review_scores_location").alias("avg_review_scores_location"),
            F.mean("review_scores_value").alias("avg_review_scores_value"),
        )
        .orderBy("avg_review_scores_rating")
    )


if __name__ == "__main__":
    df = read_parquet_data("./output/extracted_data/listing")

    selected_columns = calculate_host_ecosystem_across_cities(df)

    os.makedirs("./output/images/hosts", exist_ok=True)

    (
        px.line(
            selected_columns,
            "city",
            ["avg_host_acceptance_rate", "avg_host_response_rate"],
            title="Host Accept",
        )
        .update_layout(yaxis_title="Percentage")
        .write_html("./output/images/hosts/host_acceptance_and_response.html")
    )

    (
        px.line(
            selected_columns,
            "city",
            ["avg_host_response_time"],
            title="Host Response time",
        )
        .update_layout(yaxis_title="Hours")
        .write_html("./output/images/hosts/host_response_time.html")
    )

    (
        px.line(
            selected_columns,
            "city",
            [
                "avg_review_scores_rating",
                "avg_review_scores_accuracy",
                "avg_review_scores_cleanliness",
                "avg_review_scores_checkin",
                "avg_review_scores_communication",
                "avg_review_scores_location",
                "avg_review_scores_value",
            ],
            title="Listing Reviews",
        )
        .update_layout(yaxis_title="Rating")
        .write_html("./output/images/hosts/listing_rating.html")
    )
