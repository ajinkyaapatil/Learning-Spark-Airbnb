from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from jobs.read_data import read_parquet_data
from jobs.write import write_csv


def city_wise_data(dataframe: DataFrame) -> DataFrame:
    return (
        dataframe.filter(F.col("availability_365").cast("int") <= 365)
        .groupby("city")
        .agg(
            F.count("id").alias("count"),
            F.mean("availability_365").alias("available_365"),
            F.countDistinct("host_id").alias("hosts"),
        )
        .withColumn("listings_per_hosts", F.col("count") / F.col("hosts"))
    )


if __name__ == "__main__":
    df = read_parquet_data("./output/extracted_data/listing")

    group_by_city = city_wise_data(df)

    group_by_city.plot.bar("city", "count").write_image(
        "./output/images/city_wise/count.png"
    )
    group_by_city.plot.bar("city", "available_365").write_image(
        "./output/images/city_wise/available_365.png"
    )
    group_by_city.plot.bar("city", "listings_per_hosts").write_image(
        "./output/images/city_wise/hosts.png"
    )

    write_csv(group_by_city, "./output/city_wise_split")
