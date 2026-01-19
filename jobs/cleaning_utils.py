import pyspark.sql.functions as F


def clean_percentage_col(df, col_name):
    return (
        df.withColumn(col_name, F.regexp_replace(F.col(col_name), "%", ""))
        .withColumn(col_name, F.regexp_replace(F.col(col_name), "N/A", "0"))
        .withColumn(col_name, F.col(col_name).cast("int"))
    )


def clean_response_time_col(df, col_name):
    mapping = {
        "within an hour": "1",
        "a few days or more": "48",
        "within a day": "24",
        "within a few hours": "6",
        "N/A": "72",
    }
    return df.replace(mapping, subset=[col_name]).withColumn(
        col_name, F.col(col_name).cast("int")
    )
