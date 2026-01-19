import plotly.express as px
from pyspark.sql.functions import col, mean, mode, regexp_replace, udf
from pyspark.sql.types import StringType

from jobs.read_data import read_parquet_data


def check_host_acceptance_across_cities():
    df = read_parquet_data("./output/extracted_data/listing").fillna("0")

    replace_percentage = udf(
        lambda text: text.replace("%", "").replace("N/A", "0"), StringType()
    )

    selected_columns = (
        df.withColumn(
            "host_acceptance_rate", replace_percentage(col("host_acceptance_rate"))
        )
        .withColumn("host_acceptance_rate", col("host_acceptance_rate").cast("int"))
        .withColumn("host_response_rate", replace_percentage(col("host_response_rate")))
        .withColumn("host_response_rate", col("host_response_rate").cast("int"))
        .withColumn(
            "host_response_time", regexp_replace("host_response_time", "N/A", "72")
        )
        .withColumn(
            "host_response_time",
            regexp_replace("host_response_time", "within an hour", "1"),
        )
        .withColumn(
            "host_response_time",
            regexp_replace("host_response_time", "a few days or more", "48"),
        )
        .withColumn(
            "host_response_time",
            regexp_replace("host_response_time", "within a day", "24"),
        )
        .withColumn(
            "host_response_time",
            regexp_replace("host_response_time", "within a few hours", "6"),
        )
        .groupby("city")
        .agg(
            mean("host_acceptance_rate").alias("avg_host_acceptance_rate"),
            mean("host_response_rate").alias("avg_host_response_rate"),
            mean("host_response_time").alias("mode_host_response_time"),
        )
    )

    px.line(
        selected_columns,
        "city",
        ["avg_host_acceptance_rate", "avg_host_response_rate"],
        title="Host Accept",
    ).update_layout(yaxis_title="Percentage").write_image(
        "./output/images/hosts/host_acceptance_and_response.png"
    )

    px.line(
        selected_columns,
        "city",
        ["mode_host_response_time"],
        title="Host Accept",
    ).update_layout(yaxis_title="Hours").write_image(
        "./output/images/hosts/host_response_time.png"
    )


if __name__ == "__main__":
    check_host_acceptance_across_cities()
