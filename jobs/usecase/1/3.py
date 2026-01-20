import plotly.express as px
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from jobs.read_data import read_parquet_data

usercase = "revenue"


def select_required_columns(df: DataFrame) -> DataFrame:
    return df.select("city", "price", "occupancy")


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
        .transform(calculate_revenue)
    )

    aggregates = calculate_aggregates(listings)
    aggregates.show()

    plot_graphs(aggregates, "total_revenue", ["total_actual_revenue", "total_potential_revenue"])
    plot_graphs(aggregates, "per_property_revenue", ["median_actual_revenue", "median_potential_revenue"])


if __name__ == "__main__":
    main()
