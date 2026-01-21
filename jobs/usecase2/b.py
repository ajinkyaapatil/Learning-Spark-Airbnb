import os

import plotly.express as px
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.read_data import read_parquet_data


def select_required_columns(data_frame: DataFrame) -> DataFrame:
    return data_frame.select("id", "city", "amenities_count", "bedrooms", "bathrooms", "price")


def get_price_percentile(data_frame: DataFrame) -> DataFrame:
    return data_frame.groupBy("city").agg(
        F.expr("percentile_approx(price, 0.25)").alias("price_25"),
        F.expr("percentile_approx(price, 0.75)").alias("price_75"),
    )


def create_listing_tier_comparison(data_frame: DataFrame) -> DataFrame:
    aggregated_dataframe = get_price_percentile(data_frame)
    return data_frame.join(aggregated_dataframe, on="city", how="left").withColumn(
        "listing_tier",
        F.when(F.col("price") <= F.col("price_25"), "budget")
        .when(F.col("price") >= F.col("price_75"), "luxury")
        .otherwise("mid"),
    )


def aggregate_tier_count(data_frame: DataFrame) -> DataFrame:
    return (
        data_frame.groupBy("city", "listing_tier")
        .agg(F.avg("price").alias("listing_tier_avg_price"))
        .orderBy("listing_tier_avg_price")
    )


def save_tier_count_bar_graph():
    fig = px.bar(
        price_tier_count_dataframe, x="city", y="listing_tier_avg_price", barmode="group", color="listing_tier"
    )
    fig.update_layout(xaxis_title="City", yaxis_title="Price in dollar", title="Average tier price per city")
    fig.write_html("./output/images/budget_luxury/listing_tier_comparison.html")


def save_budget_luxury_comp_graph():
    for y_name in ["avg_amenities", "avg_bedrooms", "avg_bathrooms"]:
        fig = px.bar(
            data,
            x="city",
            y=y_name,
            color="listing_tier",
            barmode="group",
        )
        fig.write_html(f"./output/images/budget_luxury/price-tier-{y_name}.html")


def aggregate_tier_wise_comparison() -> DataFrame:
    return df.groupBy("city", "listing_tier").agg(
        F.avg("amenities_count").alias("avg_amenities"),
        F.avg("bedrooms").alias("avg_bedrooms"),
        F.avg("bathrooms").alias("avg_bathrooms"),
    )


if __name__ == "__main__":
    df = (
        read_parquet_data("./output/extracted_data/listing")
        .transform(select_required_columns)
        .transform(create_listing_tier_comparison)
    )

    price_tier_count_dataframe = aggregate_tier_count(df)

    os.makedirs("./output/images/budget_luxury/", exist_ok=True)

    save_tier_count_bar_graph()

    data = aggregate_tier_wise_comparison()

    save_budget_luxury_comp_graph()
