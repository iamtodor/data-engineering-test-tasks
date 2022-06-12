from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope="session")
def spark_session() -> SparkSession:
    return SparkSession.builder.master("local[*]").appName("test-task5").getOrCreate()
