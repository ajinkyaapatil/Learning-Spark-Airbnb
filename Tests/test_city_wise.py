import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.testing import assertDataFrameEqual

from jobs.city_wise import city_wise_data


@pytest.fixture(scope="session")
def input_schema() -> StructType:
    return StructType(
        [
            StructField("availability_365", StringType(), True),
            StructField("id", StringType(), True),
            StructField("host_id", StringType(), True),
            StructField("city", StringType(), True),
        ]
    )


@pytest.fixture(scope="session")
def extracted_schema() -> StructType:
    return StructType(
        [
            StructField("city", StringType(), True),
            StructField("count", LongType(), True),
            StructField("available_365", DoubleType(), True),
            StructField("hosts", LongType(), True),
            StructField("listings_per_hosts", DoubleType(), True),
        ]
    )


@pytest.fixture(scope="session")
def spark_session():
    spark_session = (
        SparkSession.builder.master("local[1]").appName("test_city_wise").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_remove_all_column_availability_365_with_value_greater_than_365(
    spark_session: SparkSession,
    input_schema: StructType,
    extracted_schema: StructType,
):
    input_data = [("360", "1", "1", "pune"), ("370", "2", "2", "pune")]
    output_data = [("pune", 1, 360.0, 1, 1.0)]
    input_dataframe = spark_session.createDataFrame(input_data, input_schema)
    output_dataframe = spark_session.createDataFrame(output_data, extracted_schema)

    city_wise = city_wise_data(input_dataframe)

    assertDataFrameEqual(city_wise, output_dataframe)


def test_should_return_the_count_of_listings_for_a_city(
    spark_session: SparkSession,
    input_schema: StructType,
    extracted_schema: StructType,
):
    input_data = [("60", "1", "1", "pune"), ("70", "2", "2", "pune")]

    output_data = [("pune", 2, 65.0, 2, 1.0)]
    input_dataframe = spark_session.createDataFrame(input_data, input_schema)
    output_dataframe = spark_session.createDataFrame(output_data, extracted_schema)

    city_wise = city_wise_data(input_dataframe)
    assertDataFrameEqual(city_wise, output_dataframe)


def test_should_return_listing_per_host_for_a_city(
    spark_session: SparkSession,
    input_schema: StructType,
    extracted_schema: StructType,
):
    input_data = [
        ("60", "1", "1", "pune"),
        ("60", "2", "1", "pune"),
        ("60", "2", "1", "pune"),
    ]

    output_data = [("pune", 2, 60.0, 1, 2.0)]
    input_dataframe = spark_session.createDataFrame(input_data, input_schema)
    output_dataframe = spark_session.createDataFrame(output_data, extracted_schema)

    city_wise = city_wise_data(input_dataframe)
    assertDataFrameEqual(city_wise, output_dataframe)
