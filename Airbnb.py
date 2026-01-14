import marimo

__generated_with = "0.19.0"
app = marimo.App(width="full", auto_download=["ipynb", "html"])


@app.cell
def _():
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        input_file_name,
        split,
        replace,
        udf,
        mean,
        mode,
        col,
    )
    return SparkSession, col, input_file_name, mean, split


@app.cell
def _():
    import marimo as mo
    return


@app.cell
def _(SparkSession):
    spark = (
        SparkSession.builder.appName("Airbnb Listing Overview")
        .config("spark.sql.files.maxPartitionBytes", "1MB")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "fs.file.impl.disable.cache", "true"
    )
    return (spark,)


@app.cell
def _(spark):
    spark
    return


@app.cell
def _():
    # tdf = spark.read.csv(
    #     "file:/Users/ajinkyaarvindp/Downloads/Fire.csv",
    #     header=True,
    #     inferSchema=True,
    #     quote='"',
    #     escape='"',
    #     multiLine=True,
    # )

    # tdf.show()

    # tdf.orderBy("Battalion").write_parquet.parquet(path="./output", mode="overwrite")

    # print(tdf.rdd.getNumPartitions())

    # parquetDf1 = spark.read.parquet("./output/")

    # print(parquetDf1.rdd.getNumPartitions())

    # parquetDf1.rdd.mapPartitionsWithIndex(
    #     lambda i, it: [(i, list(it)[:1])]
    # ).collect()
    return


@app.cell
def _():
    from pyspark.sql.types import StructType, StructField, StringType

    schema = StructType([
        StructField('id', StringType(), True),
        StructField('listing_url', StringType(), True),
        StructField('scrape_id', StringType(), True),
        StructField('last_scraped', StringType(), True),
        StructField('source', StringType(), True),
        StructField('name', StringType(), True),
        StructField('description', StringType(), True),
        StructField('neighborhood_overview', StringType(), True),
        StructField('picture_url', StringType(), True),

        StructField('host_id', StringType(), True),
        StructField('host_url', StringType(), True),
        StructField('host_profile_id', StringType(), True),
        StructField('host_profile_url', StringType(), True),
        StructField('host_name', StringType(), True),
        StructField('host_since', StringType(), True),
        StructField('hosts_time_as_user_years', StringType(), True),
        StructField('hosts_time_as_user_months', StringType(), True),
        StructField('hosts_time_as_host_years', StringType(), True),
        StructField('hosts_time_as_host_months', StringType(), True),
        StructField('host_location', StringType(), True),
        StructField('host_about', StringType(), True),
        StructField('host_response_time', StringType(), True),
        StructField('host_response_rate', StringType(), True),
        StructField('host_acceptance_rate', StringType(), True),
        StructField('host_is_superhost', StringType(), True),
        StructField('host_thumbnail_url', StringType(), True),
        StructField('host_picture_url', StringType(), True),
        StructField('host_neighbourhood', StringType(), True),
        StructField('host_listings_count', StringType(), True),
        StructField('host_total_listings_count', StringType(), True),
        StructField('host_verifications', StringType(), True),
        StructField('host_has_profile_pic', StringType(), True),
        StructField('host_identity_verified', StringType(), True),

        StructField('neighbourhood', StringType(), True),
        StructField('neighbourhood_cleansed', StringType(), True),
        StructField('neighbourhood_group_cleansed', StringType(), True),
        StructField('latitude', StringType(), True),
        StructField('longitude', StringType(), True),

        StructField('property_type', StringType(), True),
        StructField('room_type', StringType(), True),
        StructField('accommodates', StringType(), True),
        StructField('bathrooms', StringType(), True),
        StructField('bathrooms_text', StringType(), True),
        StructField('bedrooms', StringType(), True),
        StructField('beds', StringType(), True),
        StructField('amenities', StringType(), True),
        StructField('price', StringType(), True),

        StructField('minimum_nights', StringType(), True),
        StructField('maximum_nights', StringType(), True),
        StructField('minimum_minimum_nights', StringType(), True),
        StructField('maximum_minimum_nights', StringType(), True),
        StructField('minimum_maximum_nights', StringType(), True),
        StructField('maximum_maximum_nights', StringType(), True),
        StructField('minimum_nights_avg_ntm', StringType(), True),
        StructField('maximum_nights_avg_ntm', StringType(), True),

        StructField('calendar_updated', StringType(), True),
        StructField('has_availability', StringType(), True),
        StructField('availability_30', StringType(), True),
        StructField('availability_60', StringType(), True),
        StructField('availability_90', StringType(), True),
        StructField('availability_365', StringType(), True),
        StructField('calendar_last_scraped', StringType(), True),

        StructField('number_of_reviews', StringType(), True),
        StructField('number_of_reviews_ltm', StringType(), True),
        StructField('number_of_reviews_l30d', StringType(), True),
        StructField('availability_eoy', StringType(), True),
        StructField('number_of_reviews_ly', StringType(), True),
        StructField('estimated_occupancy_l365d', StringType(), True),
        StructField('estimated_revenue_l365d', StringType(), True),

        StructField('first_review', StringType(), True),
        StructField('last_review', StringType(), True),

        StructField('review_scores_rating', StringType(), True),
        StructField('review_scores_accuracy', StringType(), True),
        StructField('review_scores_cleanliness', StringType(), True),
        StructField('review_scores_checkin', StringType(), True),
        StructField('review_scores_communication', StringType(), True),
        StructField('review_scores_location', StringType(), True),
        StructField('review_scores_value', StringType(), True),

        StructField('license', StringType(), True),
        StructField('instant_bookable', StringType(), True),

        StructField('calculated_host_listings_count', StringType(), True),
        StructField('calculated_host_listings_count_entire_homes', StringType(), True),
        StructField('calculated_host_listings_count_private_rooms', StringType(), True),
        StructField('calculated_host_listings_count_shared_rooms', StringType(), True),
        StructField('reviews_per_month', StringType(), True)
    ])

    len(schema.fieldNames())
    return


@app.cell
def _(col, input_file_name, spark, split):
    df = (
        spark.read.csv(
            "./data/listings/",
            header=True,
            # schema=schema,
            inferSchema=True,
            quote='"',
            escape='"',
            # enforceSchema=True,

            multiLine=True,
        )
        .withColumn("city", split(input_file_name(), "/").getItem(10))
        .withColumn("city", split(col("city"), "_").getItem(0))
    )

    df.show()
    return (df,)


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
    return (regexp_replace,)


@app.cell
def _(col, df, regexp_replace):
    from pyspark.sql.functions import percent_rank, expr
    from pyspark.sql.window import Window

    windowSpec = Window.partitionBy("city").orderBy("average")

    df \
        .select("city", "price", "accommodates") \
        .withColumn("price", regexp_replace(col("price"), "[$|,]", "").cast("double")) \
        .withColumn("accommodates", col("accommodates").try_cast("integer")) \
        .withColumn("average", col("price") / col("accommodates")) \
        .dropna() \
        .groupby("city") \
        .agg(
            expr("percentile_approx(average, 0.25)").alias("p25"),
            expr("percentile_approx(average, 0.90)").alias("p90")
        )
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
