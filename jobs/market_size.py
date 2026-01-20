import os

import plotly.express as px
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.read_data import read_parquet_data


def get_market_size(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe
        .groupby("city")
        .agg(
            F.countDistinct("id").alias("number_of_listings_per_city"),
            F.countDistinct("host_id").alias("number_of_hosts_per_city"),
        )
    )


if __name__ == "__main__":
    df = read_parquet_data("./output/extracted_data/listing")

    group_by_city = get_market_size(df)


    fig_1 = px.bar(data_frame=group_by_city, x="city", y="number_of_hosts_per_city")
    fig_1.update_layout(
        xaxis_title="City",
        yaxis_title="Number of Hosts",
        title="Number of Hosts per City",
    )

    fig_2 = px.bar(data_frame=group_by_city, x="city", y="number_of_listings_per_city")
    fig_2.update_layout(
        xaxis_title="City",
        yaxis_title="Number of Listings",
        title="Number of Listings per City",
    )

    os.makedirs("./output/images/market_size", exist_ok=True)
    fig_1.write_html("./output/images/market_size/listing_count.html")
    fig_2.write_html("./output/images/market_size/host_count.html")