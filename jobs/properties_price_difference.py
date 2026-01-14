from pyspark.sql import Window
from pyspark.sql.functions import col, mean, percent_rank

from config.spark_config import spark
from jobs.read_data import read_parquet_data
from jobs.write import write_csv

currency_data = [
    ("amsterdam", 1.09),
    ("austin", 1.0),
    ("bangkok", 0.028),
    ("barcelona", 1.09),
    ("berlin", 1.09),
    ("chicago", 1.0),
    ("london", 1.27),
    ("los-angeles", 1.0),
    ("mexico-city", 0.059),
    ("new-york-city", 1.0),
    ("paris", 1.09),
    ("san-francisco", 1.0),
    ("sydney", 0.67),
    ("tokyo", 0.0067),
    ("toronto", 0.74),
]

currency_df = spark.createDataFrame(
        currency_data,
        ["city", "fx_rate"]
)

def properties_price_difference():
    df = read_parquet_data("./output/extracted_data/listing")
    properties_price_per_acc = (
        df
        .withColumn("accommodates", col("accommodates").try_cast("integer"))
        .withColumn("average", col("price") / col("accommodates"))
        .dropna()
        .groupby("city")
        .agg(mean("average").alias("average_price"))
    )

    final_df = properties_price_per_acc \
        .join(currency_df, on="city", how="left") \
        .withColumn("avg_price_usd", col("average_price") * col("fx_rate")) \
        .drop("fx_rate", "average_price") \
        .orderBy("avg_price_usd", ascending = False)

    final_df.plot.line("city", "avg_price_usd").write_image("./output/images/properties/avg_price_usd.png")

    write_csv(final_df, "./output/properties_price_difference")

def luxury_properties():
    window_spec = Window.partitionBy("city").orderBy("average")

    df = read_parquet_data("./output/extracted_data/listing")

    luxury_accommodations = (
        df
        .join(currency_df, on="city", how="left")
        .withColumn("accommodates", col("accommodates").try_cast("integer"))
        .withColumn("average", col("price") / col("accommodates"))
        .withColumn("avg_price_usd", col("average") * col("fx_rate"))
        .withColumn("PercentRank", percent_rank().over(window_spec))
        .dropna()
        .filter(col("PercentRank") > 0.9)
    )

    (luxury_accommodations
        .groupBy("city")
        .agg(mean("avg_price_usd").alias("average_price"))
        .orderBy("average_price", ascending=False)
        .plot
        .line("city", "average_price")
        .write_image("./output/images/properties/luxury_average_price.png")
     )

    write_csv(luxury_accommodations, "./output/accommodations/luxury/", partition_by="city")

def budget_properties():
    window_spec = Window.partitionBy("city").orderBy("average")

    df = read_parquet_data("./output/extracted_data/listing")

    budget_accommodations = (
        df
        .join(currency_df, on="city", how="left")
        .withColumn("accommodates", col("accommodates").try_cast("integer"))
        .withColumn("average", col("price") / col("accommodates"))
        .withColumn("avg_price_usd", col("average") * col("fx_rate"))
        .withColumn("PercentRank", percent_rank().over(window_spec))
        .dropna()
        .filter(col("PercentRank") < 0.5)
    )

    (budget_accommodations
     .groupBy("city")
     .agg(mean("avg_price_usd").alias("average_price"))
     .orderBy("average_price", ascending = False)
     .plot
     .line("city", "average_price")
     .write_image("./output/images/properties/budget_average_price.png")
     )

    write_csv(budget_accommodations, "./output/accommodations/budget/", partition_by="city")

if __name__ == '__main__':
    properties_price_difference()
    luxury_properties()
    budget_properties()