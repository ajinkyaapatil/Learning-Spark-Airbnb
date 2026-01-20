import pytest
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.testing import assertDataFrameEqual

from jobs.market_size import get_market_size


@pytest.fixture(scope="session")
def spark_session():
    spark_session = (
        SparkSession.builder.master("local[1]").appName("test_city_wise").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_market_size(spark_session: SparkSession) -> None:
    pandas_df = pd.DataFrame({
        "id": [1, 90, 1, 2],
        "city": ["Pune", "Nagpur", "Pune", "Pune"],
        "host_id": ["host_1", "host_9", "host_1", "host_2"],
    })
    expected_response = pd.DataFrame({
        "city": ["Pune", "Nagpur"],
        "number_of_listings_per_city": [2, 1],
        "number_of_hosts_per_city": [2, 1],
    })
    df = spark_session.createDataFrame(pandas_df)


    actual_response = get_market_size(df).toPandas()

    assertDataFrameEqual(actual_response, expected_response)