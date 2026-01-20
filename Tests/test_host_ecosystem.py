import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType
from pyspark.testing.utils import assertDataFrameEqual

from jobs.hosts_ecosystem import calculate_host_ecosystem_across_cities, convert_response_time_in_hours


@pytest.fixture(scope="session")
def spark_session():
    spark_session = SparkSession.builder.getOrCreate()
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="session")
def input_schema() -> StructType:
    return StructType(
        [
            StructField("city", StringType(), True),
            StructField("host_acceptance_rate", DoubleType(), True),
            StructField("host_response_rate", DoubleType(), True),
            StructField("host_response_time", StringType(), True),
            StructField("review_scores_rating", DoubleType(), True),
            StructField("review_scores_accuracy", DoubleType(), True),
            StructField("review_scores_cleanliness", DoubleType(), True),
            StructField("review_scores_checkin", DoubleType(), True),
            StructField("review_scores_communication", DoubleType(), True),
            StructField("review_scores_location", DoubleType(), True),
            StructField("review_scores_value", DoubleType(), True),
        ]
    )


@pytest.fixture(scope="session")
def extracted_schema() -> StructType:
    return StructType(
        [
            StructField("city", StringType(), True),
            StructField("avg_host_acceptance_rate", DoubleType(), True),
            StructField("avg_host_response_rate", DoubleType(), True),
            StructField("avg_host_response_time", DoubleType(), True),
            StructField("avg_review_scores_rating", DoubleType(), True),
            StructField("avg_review_scores_accuracy", DoubleType(), True),
            StructField("avg_review_scores_cleanliness", DoubleType(), True),
            StructField("avg_review_scores_checkin", DoubleType(), True),
            StructField("avg_review_scores_communication", DoubleType(), True),
            StructField("avg_review_scores_location", DoubleType(), True),
            StructField("avg_review_scores_value", DoubleType(), True),
        ]
    )


def test_convert_response_time_maps_all_categories_correctly(spark_session):
    input_data = [("within an hour",), ("within a few hours",), ("within a day",), ("a few days or more",), ("N/A",)]
    schema = StructType([StructField("host_response_time", StringType(), True)])
    df = spark_session.createDataFrame(input_data, schema)

    expected_data = [(1,), (6,), (24,), (48,), (72,)]
    expected_schema = StructType([StructField("host_response_time", IntegerType(), True)])
    expected_df = spark_session.createDataFrame(expected_data, expected_schema)

    result_df = convert_response_time_in_hours(df)

    assertDataFrameEqual(result_df, expected_df)


def test_calculate_host_ecosystem_averages_calculation(spark_session, input_schema, extracted_schema):
    input_data = [
        ("Paris", 0.8, 100.0, "within an hour", 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0),
        ("Paris", 0.4, 50.0, "within a day", 3.0, 3.0, 3.0, 3.0, 3.0, 3.0, 3.0),
    ]
    df = spark_session.createDataFrame(input_data, input_schema)

    expected_data = [("Paris", 0.6, 75.0, 12.5, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0)]
    expected_df = spark_session.createDataFrame(expected_data, extracted_schema)

    result_df = calculate_host_ecosystem_across_cities(df)

    assertDataFrameEqual(result_df, expected_df)


def test_calculate_host_ecosystem_sort_order(spark_session, input_schema, extracted_schema):
    input_data = [
        ("City A", 1.0, 100.0, "within an hour", 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0),
        ("City B", 1.0, 100.0, "within an hour", 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0),
    ]
    df = spark_session.createDataFrame(input_data, input_schema)

    expected_data = [
        ("City B", 1.0, 100.0, 1.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0),
        ("City A", 1.0, 100.0, 1.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0, 5.0),
    ]
    expected_df = spark_session.createDataFrame(expected_data, extracted_schema)

    result_df = calculate_host_ecosystem_across_cities(df)

    assertDataFrameEqual(result_df, expected_df)
