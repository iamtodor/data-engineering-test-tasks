from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Job:
    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.master("local[*]").config("spark.driver.memory", "2g").appName("task4").getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        self.input_directory = "../data/input"
        self.dimensions = ["datehour", "domain", "user.country"]
        self.metrics = [
            "pageviews",
            # 'pageviews_with_consent',
            "consents_asked",
            # 'consents_asked_with_consent',
            "consents_asked_with_consent",
            # 'consents_given_with_consent',
            "avg_pageviews_per_user",
        ]
        self.__init_schema()

    def __init_schema(self) -> None:
        self.__schema = StructType(
            [
                StructField("datetime", StringType(), True),
                StructField("id", StringType(), True),
                StructField("type", StringType(), True),
                StructField("domain", StringType(), True),
                StructField("gender", StringType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )

    def execute(self) -> None:
        data = self.__load_data()
        data = self.drop_duplicates(data)
        data = self.extract_event_type(data, "pageviews", "pageview")
        data = self.extract_event_type(data, "consents_asked", "consents.asked")
        data = self.extract_event_type(data, "consents_given", "consents.given")
        # data = self.calculate_avg_pageviews_per_user(data)
        data.show(truncate=False)
        data.printSchema()

        output = data.groupBy(self.dimensions).agg(
            F.count("pageviews").alias("pageviews"),
            F.count("consents_asked").alias("consents_asked"),
            F.count("consents_given").alias("consents_given"),
            F.round(F.avg("pageviews"), 2).alias("avg_pageviews_per_user"),
        )

        self.save_data(output)

    def __load_data(self) -> DataFrame:
        data = self.spark.read.format("json").option("inferSchema", "true").load(f"{self.input_directory}")
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
