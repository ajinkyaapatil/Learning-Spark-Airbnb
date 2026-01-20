import plotly.express as px
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from jobs.read_data import read_parquet_data

usercase = "revenue"


def select_required_columns(df: DataFrame) -> DataFrame:
    return df.select("city", "price", "occupancy")


def convert_price_to_usd(df: DataFrame) -> DataFrame:
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

    c = F.when(F.col("city") == "", 1.0)
    for city, rate in conversion_rates.items():
        c = c.when(F.col("city") == city, rate)
    c.otherwise(1.0)

    return df.withColumn("price", c * F.col("price"))


def calculate_revenue(df: DataFrame) -> DataFrame:
    return df.withColumns(
        {
            "actual_revenue": F.col("price") * F.col("occupancy"),
            "potential_revenue": F.col("price") * F.lit(365),
        }
    )


def calculate_aggregates(df: DataFrame) -> DataFrame:
    return (
        df.groupby("city")
        .agg(
            F.mean("price").alias("mean_price"),
            F.median("price").alias("median_price"),
            F.mean("occupancy").alias("mean_occupancy"),
            F.median("occupancy").alias("median_occupancy"),
            F.median("potential_revenue").alias("median_potential_revenue"),
            F.median("actual_revenue").alias("median_actual_revenue"),
            F.sum("potential_revenue").alias("total_potential_revenue"),
            F.sum("actual_revenue").alias("total_actual_revenue"),
        )
        .cache()
    )


def _get_maxes(df: DataFrame, columns: list[str]) -> dict[str, float]:
    return df.select(*[F.max(column).alias(column) for column in columns]).collect()[0].asDict()


def plot_graphs(df: DataFrame, title: str, columns: list[str]):
    max_values = _get_maxes(df, columns)

    plot = px.line(df, x="city", y=columns, title=title)
    for column in columns:
        plot.add_hline(y=max_values[column], line_dash="dash", annotation={"text": f"max {column}"})

    plot.write_image(f"./output/images/{usercase}/{title}.png")


def main():
    listings = (
        read_parquet_data("./output/extracted_data/listing")
        .transform(select_required_columns)
        .transform(convert_price_to_usd)
        .transform(calculate_revenue)
    )

    aggregates = calculate_aggregates(listings)
    aggregates.show()

    plot_graphs(aggregates, "total_revenue", ["total_actual_revenue", "total_potential_revenue"])
    plot_graphs(aggregates, "per_property_revenue", ["median_actual_revenue", "median_potential_revenue"])


if __name__ == "__main__":
    main()
