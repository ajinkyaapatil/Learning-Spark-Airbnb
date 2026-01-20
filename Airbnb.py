import marimo

__generated_with = "0.19.0"
app = marimo.App(width="full", auto_download=["ipynb", "html"])


@app.cell
def _():
    from pyspark.sql import SparkSession
    from pyspark.sql.types import DateType, DoubleType
    import pyspark.sql.functions as F
    return DoubleType, F, SparkSession


@app.cell
def _():
    import marimo as mo
    return


@app.cell
def _(SparkSession):
    spark = SparkSession.builder.appName("Airbnb Listing Overview") \
        .master("local[*]") \
        .getOrCreate()
    return (spark,)


@app.cell
def _(DoubleType, F, spark):
    df = (
        spark.read.csv(
            "./data/listings/",
            header=True,
            inferSchema=True,
            quote='"',
            escape='"',
            multiLine=True,
        )
        .withColumn(
            "filename",
            F.split(F.input_file_name(), "/").getItem(
                F.size(F.split(F.input_file_name(), "/")) - 1
            ),
        )
        .withColumns(
            {
                "city": F.split("filename", "_").getItem(1),
                "price": F.try_to_number("price", F.lit("$999,999,999.99")).cast(DoubleType()),
            }
        ).filter(F.col("price").isNotNull())
        .select("id", "price", "last_scraped", "city")
    )

    df.show()
    return (df,)


@app.cell
def _(F, df):
    from pyspark.sql.window import Window

    a = df.withColumn("rn", F.row_number().over(Window.partitionBy("id").orderBy(F.col("last_scraped").desc()))).filter(F.col("rn") == 1).drop("rn")
    a.show()
    return (a,)


@app.cell
def _(col, df, mean):
    from pyspark.sql.functions import regexp_replace

    df \
        .select("city", "price", "accommodates") \
        .withColumn("price", regexp_replace(col("price"), "[$|,]", "").cast("double")) \
        .withColumn("accommodates", col("accommodates").try_cast("integer")) \
        .withColumn("average", col("price") / col("accommodates")) \
        .dropna() \
        .groupby("city") \
        .agg(mean("average")) \
        .explain(True)
    return


@app.cell
def _():
    # from pyspark.sql.functions import percent_rank, expr
    # from pyspark.sql.window import Window

    # windowSpec = Window.partitionBy("city").orderBy("average")

    # df \
    #     .select("city", "price", "accommodates") \
    #     .withColumn("price", regexp_replace(col("price"), "[$|,]", "").cast("double")) \
    #     .withColumn("accommodates", col("accommodates").try_cast("integer")) \
    #     .withColumn("average", col("price") / col("accommodates")) \
    #     .dropna() \
    #     .groupby("city") \
    #     .agg(
    #         expr("percentile_approx(average, 0.25)").alias("p25"),
    #         expr("percentile_approx(average, 0.90)").alias("p90")
    #     )
    return


@app.cell
def _(a):
    a.plot.bar(x='PercentRank', y='PercentRank')
    return


@app.cell
def _(df):
    df \
        .select("city", "bedrooms", "price") \
        .dropna() \
        .count()
    return


@app.cell
def _():
    # spark.stop()
    return


if __name__ == "__main__":
    app.run()
