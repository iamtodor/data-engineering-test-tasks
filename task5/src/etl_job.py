from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType


class Job:
    def __init__(self) -> None:
        self.__spark = (
            SparkSession.builder.master("local[*]").appName("task5").getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")
        self.__dimensions = ["datehour", "domain", "user.country"]

    @staticmethod
    def __init_schema() -> StructType:
        return StructType(
            [
                StructField("datetime", DateType(), True),
                StructField("id", StringType(), True),
                StructField("type", StringType(), True),
                StructField("domain", StringType(), True),
                StructField(
                    "user",
                    StructType(
                        [
                            StructField("id", StringType(), True),
                            StructField("token", StringType(), True),
                            StructField("country", StringType(), True),
                        ]
                    ),
                ),
            ]
        )

    def execute(self) -> None:
        df_schema = self.__init_schema()
        data = self.load_data(df_schema)
        data = self.drop_duplicates(data)
        data = self.extract_event_type(data, "pageviews", "pageview")
        data = self.extract_event_type(data, "consents_asked", "consent.asked")
        data = self.extract_event_type(data, "consents_given", "consent.given")
        data = self.aggregate(data)

        self.save_data(data)

    def load_data(self, schema: StructType, input_format: str = "json",
                  input_location: str = "../data/input") -> DataFrame:
        return (
            self.__spark.read.format(input_format)
                .schema(schema)
                .load(input_location)
        )

    @staticmethod
    def drop_duplicates(data: DataFrame) -> DataFrame:
        return data.drop_duplicates(["id"])

    @staticmethod
    def extract_event_type(data: DataFrame, new_column_name: str, event_type: str) -> DataFrame:
        return data.withColumn(new_column_name, F.when(F.col("type") == event_type, 1).otherwise(0))

    def aggregate(self, data: DataFrame) -> DataFrame:
        return data.groupBy(self.__dimensions).agg(
            F.count("pageviews").alias("pageviews"),
            F.count("consents_asked").alias("consents_asked"),
            F.count("consents_given").alias("consents_given"),
            F.round(F.avg("pageviews"), 2).alias("avg_pageviews_per_user"),
        )

    @staticmethod
    def save_data(result: DataFrame, output_format: str = "parquet", output_location: str = "../data/output") -> None:
        assert output_format in ["parquet", "csv"]
        result.coalesce(1).write.format(output_format).option("header", "true").mode("overwrite").save(output_location)
