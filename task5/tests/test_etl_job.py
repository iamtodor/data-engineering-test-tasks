from typing import Generator

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from task5.src.etl_job import Job


@pytest.fixture(scope="session")
def spark_session() -> Generator:
    spark_session = SparkSession.builder.master("local[*]").appName("some-app-name").getOrCreate()

    yield spark_session
    spark_session.stop()


def test_drop_duplicates(spark_session: SparkSession) -> None:
    input_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("domain", StringType(), True),
        ]
    )
    input_data = [
        ("1", "www.domain-A.eu"),
        ("2", "www.domain-A.eu"),
        ("1", "www.domain-A.eu"),
        ("3", "www.domain-A.eu"),
    ]
    input_df = spark_session.createDataFrame(data=input_data, schema=input_schema)
    data = Job.drop_duplicates(input_df)
    assert input_df.count() == 4
    assert data.count() == 3


def test_extract_event_type(spark_session: SparkSession) -> None:
    input_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
        ]
    )
    input_data = [
        ("1", "pageview"),
        ("2", "pageview"),
        ("3", "consent.asked"),
        ("4", "consent.asked"),
        ("5", "pageview"),
        ("6", "consent.asked"),
        ("7", "pageview"),
        ("8", "consents.given"),
        ("9", "consents.given"),
        ("10", "consents.given"),
        ("11", "consents.given"),
    ]

    input_df = spark_session.createDataFrame(data=input_data, schema=input_schema)

    data = Job.extract_event_type(input_df, "pageviews", "pageview")
    data = Job.extract_event_type(data, "consents_asked", "consent.asked")
    data = Job.extract_event_type(data, "consents_given", "consents.given")

    assert data.groupBy("pageviews").count().where(F.col("pageviews") == 1).collect()[0].asDict()["count"] == 4
    assert data.groupBy("consents_asked").count().where(F.col("consents_asked") == 1).collect()[0].asDict()["count"] == 3
    assert data.groupBy("consents_given").count().where(F.col("consents_given") == 1).collect()[0].asDict()["count"] == 4
