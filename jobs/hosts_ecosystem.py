import plotly.express as px
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType

from jobs.read_data import read_parquet_data

response_time_to_hours = {
    "within an hour": "1",
    "a few days or more": "48",
    "within a day": "24",
    "within a few hours": "6",
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
        )
    )


if __name__ == "__main__":
    df = read_parquet_data("./output/extracted_data/listing")

    selected_columns = calculate_host_ecosystem_across_cities(df)
    px.line(
        selected_columns,
        "city",
        ["avg_host_acceptance_rate", "avg_host_response_rate"],
        title="Host Accept",
    ).update_layout(yaxis_title="Percentage").write_image("./output/images/hosts/host_acceptance_and_response.png")
    px.line(
        selected_columns,
        "city",
        ["avg_host_response_time"],
        title="Host Response time",
    ).update_layout(yaxis_title="Hours").write_image("./output/images/hosts/host_response_time.png")
