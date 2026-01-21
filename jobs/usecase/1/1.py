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


def extract_calendar_month(dataframe: DataFrame) -> DataFrame:
    return dataframe.withColumn("booking_month", F.date_trunc("MON", F.col("date")))


def get_median_booking_by_city(data_frame: DataFrame) -> DataFrame:
    return data_frame.groupby("city", "booking_month", "listing_id") \
        .agg(F.count("listing_id").alias("booking_count"), ) \
        .groupby("city", "booking_month") \
        .agg(F.avg("booking_count").alias("avg_booking_per_listing")) \
        .orderBy("city", "booking_month")


def save_listing_count(dataframe: DataFrame) -> None:
    fig_1 = px.bar(data_frame=dataframe, x="city", y="number_of_hosts_per_city")
    fig_1.update_layout(
        xaxis_title="City",
        yaxis_title="Number of Hosts",
        title="Number of Hosts per City",
    )
    fig_1.write_html("./output/images/market_size/listing_count.html")


def save_host_count(dataframe: DataFrame) -> None:
    fig_2 = px.bar(data_frame=dataframe, x="city", y="number_of_listings_per_city")
    fig_2.update_layout(
        xaxis_title="City",
        yaxis_title="Number of Listings",
        title="Number of Listings per City",
    )
    fig_2.write_html("./output/images/market_size/host_count.html")


def save_average_booking_per_month():
    fig = px.line(data_frame=booking_by_month, x="booking_month", y="avg_booking_per_listing", color="city")
    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Average Booking per Listing",
        title="Average Booking per Listing per City per Month",
    )
    fig.write_html("./output/images/market_size/booking_by_month.html")


if __name__ == "__main__":
    df = read_parquet_data("./output/extracted_data/listing")
    calendar_df = read_parquet_data("./output/extracted_data/calendar").transform(extract_calendar_month)

    booking_by_month = get_median_booking_by_city(calendar_df)
    market_size_by_city = get_market_size(df)

    os.makedirs("./output/images/market_size", exist_ok=True)
    save_listing_count(market_size_by_city)
    save_host_count(market_size_by_city)
    save_average_booking_per_month()

