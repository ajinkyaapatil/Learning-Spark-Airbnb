import plotly.express as px
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType, IntegerType

from jobs.read_data import read_parquet_data


usercase = "sameprice"


def select_required_columns(df: DataFrame) -> DataFrame:
    return df.select("city", "price", "accommodates", "bedrooms", "bathrooms", "amenities")


def add_bin_number(df: DataFrame) -> DataFrame:
    return df.withColumn("bin", F.when(F.col("price") < 1, 0).otherwise(F.log10("price").cast(IntegerType()) + 1))


def add_number_of_amenities(df: DataFrame) -> DataFrame:
    return df.withColumn("amenities", F.size(F.split("amenities", ",")).cast(DoubleType()))


def _get_maxes(df: DataFrame, columns: list[str]) -> dict[str, float]:
    return df.select(*[F.max(column).alias(column) for column in columns]).collect()[0].asDict()


def plot_graphs(df: DataFrame, title: str, columns: list[str]):
    max_values = _get_maxes(df, columns)

    plot = px.line(df, x="city", y=columns, title=title)
    for column in columns:
        plot.add_hline(y=max_values[column], line_dash="dash", annotation={"text": f"max {column}"})

    plot.write_html(f"./output/images/{usercase}/{title}.html")


def main():
    listings = (
        read_parquet_data("./output/extracted_data/listing")
        .transform(select_required_columns)
        .transform(add_bin_number)
        .transform(add_number_of_amenities)
    ).cache()

    bins = listings.select("bin").distinct().collect()
    for row in bins:
        b = row["bin"]
        aggregates = (
            listings.filter(F.col("bin") == b)
            .groupBy("city")
            .agg(
                F.mean("accommodates").alias("mean_accomodates"),
                F.median("accommodates").alias("median_accomodates"),
                F.mean("bathrooms").alias("mean_bathrooms"),
                F.median("bathrooms").alias("median_bathrooms"),
                F.mean("bedrooms").alias("mean_bedrooms"),
                F.median("bedrooms").alias("median_bedrooms"),
                F.mean("amenities").alias("mean_amenities"),
                F.median("amenities").alias("median_amenities"),
            )
        )

        s = 10**b
        e = 10**b * 10
        plot_graphs(aggregates, f"accomodates_{s}-{e}", ["mean_accomodates", "median_accomodates"])
        plot_graphs(
            aggregates,
            f"bed_bath_{s}-{e}",
            ["mean_bathrooms", "median_bathrooms", "mean_bedrooms", "median_bedrooms"],
        )
        plot_graphs(aggregates, f"amenities_{s}-{e}", ["mean_amenities", "median_amenities"])


if __name__ == "__main__":
    main()
