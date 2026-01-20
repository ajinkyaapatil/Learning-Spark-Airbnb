import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from jobs.hosts_ecosystem import convert_response_time_in_hours, replace_na_with_zero


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("cleaning_utils_test").getOrCreate()


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
    expected_df = spark.createDataFrame(expected_data, StructType([StructField("col", IntegerType(), True)]))

    cleaned_df = convert_response_time_in_hours(df, "col")

    assertDataFrameEqual(cleaned_df, expected_df)
