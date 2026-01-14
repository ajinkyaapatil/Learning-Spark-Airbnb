from pyspark.sql.functions import udf, col, regexp_replace, mean, mode
from pyspark.sql.types import StringType

from jobs.read_data import read_parquet_data
from jobs.write import write_csv


def check_host_acceptance_across_cities():
    df = read_parquet_data("./output/extracted_data/listing").fillna("0")

    replace_percentage = udf(
        lambda text: text.replace("%", "").replace("N/A", "0"), StringType()
    )

    host_acceptance_rate = df.withColumn("host_acceptance_rate", replace_percentage(col("host_acceptance_rate"))) \
        .withColumn("host_acceptance_rate", col("host_acceptance_rate").cast("int"))

    host_response_rate = host_acceptance_rate.withColumn("host_response_rate",
                                                         replace_percentage(col("host_response_rate"))) \
        .withColumn("host_response_rate", col("host_response_rate").cast("int"))

    host_response_time = host_response_rate.withColumn(
        "host_response_time", regexp_replace("host_response_time", "N/A", "72")
    ) \
        .withColumn(
        "host_response_time",
        regexp_replace("host_response_time", "within an hour", "1"),
    ) \
        .withColumn(
        "host_response_time",
        regexp_replace("host_response_time", "a few days or more", "48"),
    ) \
        .withColumn(
        "host_response_time",
        regexp_replace("host_response_time", "within a day", "24"),
    ) \
        .withColumn(
        "host_response_time",
        regexp_replace("host_response_time", "within a few hours", "6"),
    )

    selected_columns = (
        host_response_time
        .groupby("city")
        .agg(
            mean("host_acceptance_rate").alias("avg_host_acceptance_rate"),
            mean("host_response_rate").alias("avg_host_response_rate"),
            mode("host_response_time").alias("mode_host_response_time"),
        )
        .withColumn("host_ecosystem", col("avg_host_response_rate") * col("avg_host_acceptance_rate") / 10000)
        .orderBy("host_ecosystem")
    )

    selected_columns.plot.line("city", "host_ecosystem").write_image("./output/images/hosts/host_ecosystem.png")

    write_csv(selected_columns, path="./output/host_acceptance")

if __name__ == '__main__':
    check_host_acceptance_across_cities()