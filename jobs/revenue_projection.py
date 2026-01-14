
from pyspark.sql.functions import mean, col, count

from jobs.read_data import read_parquet_data
from jobs.write import write_csv


def calculate_revenue_projection():
    df = read_parquet_data("./output/extracted_data/listing")

    revenue_projected = (
        df
        .withColumn("accommodates", col("accommodates").try_cast("integer"))
        .fillna(1, subset=["price", "accommodates"])
        .withColumn("average", col("price") / col("accommodates"))
        .groupBy("city")
        .agg(mean("average"))
    )

    revenue_projected.show(20)

    # write_csv(revenue_projected, "./output/extracted_data/revenue_projection")


if __name__ == '__main__':
    calculate_revenue_projection()
