from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import plotly.express as px

from jobs.read_data import read_parquet_data


def select_required_columns(data_frame: DataFrame) -> DataFrame:
    return data_frame.select("id", "city", "amenities_count", "bedrooms", "bathrooms", "price")


def get_price_percentile(data_frame: DataFrame) -> DataFrame:
    return data_frame.groupBy("city").agg(
        F.expr("percentile_approx(price, 0.25)").alias("price_25"),
        F.expr("percentile_approx(price, 0.75)").alias("price_75")
    )


def create_listing_tier_comparison(data_frame: DataFrame) -> DataFrame:
    aggregated_dataframe = get_price_percentile(data_frame)
    return data_frame.join(aggregated_dataframe, on="city", how="left") \
        .withColumn(
        "listing_tier",
        F.when(F.col("price") <= F.col("price_25"), "budget")
        .when(F.col("price") >= F.col("price_75"), "luxury")
        .otherwise("mid")
    )


def aggregate_tier_count(data_frame: DataFrame) -> DataFrame:
    return data_frame.groupBy("city", "listing_tier").agg(F.count("listing_tier").alias("listing_tier_count")).orderBy("city",
                                                                                                               "listing_tier")


def save_tier_count_bar_graph():
    fig = px.bar(price_tier_count_dataframe, x="city", y="listing_tier_count", barmode="group", color="listing_tier")
    fig.update_layout(xaxis_title="City", yaxis_title="Listing Tier Count", title="Listing tier count per city")
    fig.write_html("./output/images/listing_tier_comparison.html")



if __name__ == "__main__":
    df = read_parquet_data("./output/extracted_data/listing").transform(select_required_columns).transform(
        create_listing_tier_comparison)

    price_tier_count_dataframe = aggregate_tier_count(df)
    save_tier_count_bar_graph()

    data = df.groupBy("city_name", "price_tier").agg(
        F.avg("amenities_count").alias("avg_amenities"),
        F.avg("bedrooms").alias("avg_bedrooms"),
        F.avg("bathrooms").alias("avg_bathrooms"),
    )

    for y_name in ["avg_amenities", "avg_bedrooms", "avg_bathrooms", "avg_rating"]:
        fig = px.bar(
            data,
            x="city_name",
            y=y_name,
            color="price_tier",
            barmode="group",
        )
        fig.write_html(f"./output/images/price-tier-{y_name}.html")




