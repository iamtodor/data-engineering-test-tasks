import os
from pathlib import PosixPath
from typing import List, Tuple

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from task5.src.etl_job import Job


@pytest.fixture
def schema() -> StructType:
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
        ]
    )


@pytest.fixture
def data() -> List[Tuple[str, str]]:
    return [("1", "foo"), ("2", "bar"), ("3", "baz")]


def test_save_data(
    schema: StructType, data: List[Tuple[str, str]], tmp_path: PosixPath, spark_session: SparkSession
) -> None:
    input_df = spark_session.createDataFrame(data=data, schema=schema)

    output_location = tmp_path.joinpath("output")
    output_format = "parquet"
    Job.save_data(input_df, output_format, str(output_location))
    assert os.path.exists(output_location)

    output_files = os.listdir(output_location)
    file_extensions = [os.path.splitext(filename)[1] for filename in output_files]
    assert sum(output_format in str(file_extension) for file_extension in file_extensions) == 1


def test_load_data(
    schema: StructType, data: List[Tuple[str, str]], tmp_path: PosixPath, spark_session: SparkSession
) -> None:
    input_df = spark_session.createDataFrame(data=data, schema=schema)

    data_location = tmp_path.joinpath("output")
    data_format = "parquet"
    Job.save_data(input_df, data_format, str(data_location))

    job = Job()
    loaded_data = job.load_data(schema, data_format, str(data_location))
    assert loaded_data.count() == 3
    assert len(loaded_data.columns) == 2
    assert set(loaded_data.columns) == {"id", "type"}


def test_schema(spark_session: SparkSession) -> None:
    schema = Job.init_schema()
    assert len(schema.fields) == 5
    assert set(schema.fieldNames()) == {"datetime", "id", "type", "domain", "user"}
    user_nested_structure = [field for field in schema.jsonValue()["fields"] if field["name"] == "user"]
    user_nested_fields = user_nested_structure[0]["type"]["fields"]
    assert len(user_nested_fields) == 3
    user_nested_fields = [field["name"] for field in user_nested_fields]
    assert set(user_nested_fields) == {"id", "token", "country"}


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
    assert (
        data.groupBy("consents_asked").count().where(F.col("consents_asked") == 1).collect()[0].asDict()["count"] == 3
    )
    assert (
        data.groupBy("consents_given").count().where(F.col("consents_given") == 1).collect()[0].asDict()["count"] == 4
    )
