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

    output_data = [("pune", 3, 60.0, 1, 3.0)]
    input_dataframe = spark_session.createDataFrame(input_data, input_schema)
    output_dataframe = spark_session.createDataFrame(output_data, extracted_schema)

    city_wise = city_wise_data(input_dataframe)
    assertDataFrameEqual(city_wise, output_dataframe)


def test_multiple_cities_grouping(
    spark_session: SparkSession,
    input_schema: StructType,
    extracted_schema: StructType,
):
    input_data = [
        ("200", "1", "1", "Pune"),
        ("250", "2", "2", "Pune"),
        ("300", "3", "3", "Mumbai"),
        ("310", "4", "4", "Mumbai"),
    ]
    expected_data = [
        ("Mumbai", 2, 305.0, 2, 1.0),
        ("Pune", 2, 225.0, 2, 1.0),
    ]
    input_df = spark_session.createDataFrame(input_data, input_schema)
    expected_df = spark_session.createDataFrame(expected_data, extracted_schema)
    result_df = city_wise_data(input_df).orderBy("city")
    expected_df = expected_df.orderBy("city")
    assertDataFrameEqual(result_df, expected_df)


def test_boundary_value_analysis_365(
    spark_session: SparkSession,
    input_schema: StructType,
    extracted_schema: StructType,
):
    input_data = [("365", "1", "1", "pune")]
    expected_data = [("pune", 1, 365.0, 1, 1.0)]
    input_df = spark_session.createDataFrame(input_data, input_schema)
    expected_df = spark_session.createDataFrame(expected_data, extracted_schema)
    result_df = city_wise_data(input_df)
    assertDataFrameEqual(result_df, expected_df)


def test_empty_input_handling(
    spark_session: SparkSession,
    input_schema: StructType,
    extracted_schema: StructType,
):
    empty_input_df = spark_session.createDataFrame([], input_schema)
    empty_expected_df = spark_session.createDataFrame([], extracted_schema)
    result_df = city_wise_data(empty_input_df)
    assertDataFrameEqual(result_df, empty_expected_df)
