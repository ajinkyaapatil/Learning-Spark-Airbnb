import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from jobs.hosts_ecosystem import clean_percentage_col, clean_response_time_col


@pytest.fixture(scope="module")
def spark():
    return (
        SparkSession.builder.master("local[1]")
        .appName("cleaning_utils_test")
        .getOrCreate()
    )


def test_clean_percentage_col(spark):
    schema = StructType([StructField("col", StringType(), True)])
    data = [("50%",), ("N/A",), ("42",), ("75%",), ("N/A",)]
    df = spark.createDataFrame(data, schema)
    expected_data = [(50,), (0,), (42,), (75,), (0,)]
    expected_df = spark.createDataFrame(
        expected_data, StructType([StructField("col", IntegerType(), True)])
    )

    cleaned_df = clean_percentage_col(df, "col")

    assertDataFrameEqual(cleaned_df, expected_df)


def test_clean_response_time_col(spark):
    schema = StructType([StructField("col", StringType(), True)])
    data = [
        ("within an hour",),
        ("a few days or more",),
        ("within a day",),
        ("within a few hours",),
        ("N/A",),
        ("72",),
    ]
    df = spark.createDataFrame(data, schema)
    expected_data = [(1,), (48,), (24,), (6,), (72,), (72,)]
    expected_df = spark.createDataFrame(
        expected_data, StructType([StructField("col", IntegerType(), True)])
    )

    cleaned_df = clean_response_time_col(df, "col")

    assertDataFrameEqual(cleaned_df, expected_df)
