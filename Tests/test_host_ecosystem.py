import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.testing import assertDataFrameEqual

from jobs.hosts_ecosystem import calculate_host_ecosystem_across_cities


@pytest.fixture(scope="session")
def input_schema() -> StructType:
    return StructType(
        [
            StructField("city", StringType(), True),
            StructField("host_acceptance_rate", StringType(), True),
            StructField("host_response_rate", StringType(), True),
            StructField("host_response_time", StringType(), True),
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
        ]
    )


@pytest.fixture(scope="session")
def spark_session():
    spark_session = SparkSession.builder.master("local[1]").appName("test_host_ecosystem").getOrCreate()
    yield spark_session
    spark_session.stop()


def test_aggregation_multiple_cities(
    spark_session: SparkSession,
    input_schema: StructType,
    extracted_schema: StructType,
):
    input_data = [
        ("cityA", "80", "90", "within an hour"),
        ("cityA", "0", "80", "a few days or more"),
        ("cityB", "100", "100", "within a day"),
        ("cityB", "100", "0", "within a few hours"),
    ]
    output_data = [
        ("cityA", 40.0, 85.0, 24.5),
        ("cityB", 100.0, 50.0, 15.0),
    ]

    df = spark_session.createDataFrame(input_data, input_schema)
    output_dataset = spark_session.createDataFrame(output_data, extracted_schema)

    host_ecosystem_data = calculate_host_ecosystem_across_cities(df).orderBy("city")

    expected_dataset = output_dataset.orderBy("city")

    assertDataFrameEqual(host_ecosystem_data, expected_dataset)
