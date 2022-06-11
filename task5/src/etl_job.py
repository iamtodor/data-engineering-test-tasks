from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType


class Job:
    def __init__(self) -> None:
        self.__spark = (
            SparkSession.builder.master("local[*]").config("spark.driver.memory", "2g").appName("task4").getOrCreate()
        )
        self.__spark.sparkContext.setLogLevel("WARN")
        self.__input_directory = "../data/input"
        self.__dimensions = ["datehour", "domain", "user.country"]
        self.__init_schema()

    def __init_schema(self) -> None:
        self.__schema = StructType(
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
        data = self.__load_data()
        data = self.drop_duplicates(data)
        data = self.extract_event_type(data, "pageviews", "pageview")
        data = self.extract_event_type(data, "consents_asked", "consent.asked")
        data = self.extract_event_type(data, "consents_given", "consent.given")
        data = self.aggregate(data)

        self.save_data(data)

    def __load_data(self) -> DataFrame:
        data = (
            self.__spark.read.format("json")
            .option("inferSchema", "true")
            .schema(self.__schema)
            .load(f"{self.__input_directory}")
        )
        return data

    @staticmethod
    def drop_duplicates(data: DataFrame) -> DataFrame:
        return data.drop_duplicates(["id"])

    @staticmethod
    def extract_event_type(data: DataFrame, new_column_name: str, event_type: str) -> DataFrame:
        return data.withColumn(new_column_name, F.when(F.col("type") == event_type, 1).otherwise(0))

    @staticmethod
    def save_data(result: DataFrame) -> None:
        result.coalesce(1).write.format("parquet").option("header", "true").mode("overwrite").save("../data/output")

    def aggregate(self, data: DataFrame) -> DataFrame:
        return data.groupBy(self.__dimensions).agg(
            F.count("pageviews").alias("pageviews"),
            F.count("consents_asked").alias("consents_asked"),
            F.count("consents_given").alias("consents_given"),
            F.round(F.avg("pageviews"), 2).alias("avg_pageviews_per_user"),
        )
