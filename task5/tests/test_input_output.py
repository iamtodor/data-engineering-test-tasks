import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from task5.src.etl_job import Job


@pytest.fixture
def schema():
    return StructType(
        [
            StructField("id", StringType(), True),
            StructField("type", StringType(), True),
        ]
    )


@pytest.fixture
def data():
    return [
        ("1", "foo"),
        ("2", "bar"),
        ("3", "baz")
    ]


def test_save_data(schema, data, tmp_path, spark_session: SparkSession) -> None:
    input_df = spark_session.createDataFrame(data=data, schema=schema)

    output_location = tmp_path.joinpath("output")
    output_format = "parquet"
    Job.save_data(input_df, output_format, str(output_location))
    assert os.path.exists(output_location)

    output_files = os.listdir(output_location)
    file_extensions = [os.path.splitext(filename)[1] for filename in output_files]
    assert sum(output_format in str(file_extension) for file_extension in file_extensions) == 1


def test_load_data(schema, data, tmp_path, spark_session: SparkSession) -> None:
    input_df = spark_session.createDataFrame(data=data, schema=schema)

    data_location = tmp_path.joinpath("output")
    data_format = "parquet"
    Job.save_data(input_df, data_format, str(data_location))

    job = Job()
    loaded_data = job.load_data(schema, data_format, str(data_location))
    assert loaded_data.count() == 3
    assert len(loaded_data.columns) == 2
    assert 'id' in loaded_data.columns
    assert 'type' in loaded_data.columns
