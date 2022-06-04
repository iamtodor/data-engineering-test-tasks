from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from task4.constants import JDBC_URL, POSTGRES_USER, POSTGRES_PASS, POSTGRES_TABLE


class Solution:
    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.master("local[*]")
            .config("spark.driver.memory", "2g")
            .config("spark.jars", "jars/postgresql-42.3.6.jar")
            .appName("task4")
            .getOrCreate()
        )
        self.spark.sparkContext.setLogLevel("WARN")
        self.input_directory = "data"

    def execute(self) -> None:
        data = self.load_data()
        data = data.withColumn("date", F.to_date(F.from_unixtime(F.col("timestamp"))))
        data = data.drop("timestamp")
        data = data.select("date", "user_id", "user_answer", "answered_correctly")

        self.ingest_data(data)

    def load_data(self):
        data = (
            self.spark.read.format("parquet")
            .option("header", "true")
            .load(f"{self.input_directory}/data.parquet")
            .select("timestamp", "user_id", "user_answer", "answered_correctly")
        )
        return data

    @staticmethod
    def ingest_data(data):
        (
            data.write.mode("overwrite")
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("driver", "org.postgresql.Driver")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASS)
            .option("dbtable", POSTGRES_TABLE)
            .save()
        )


if __name__ == "__main__":
    solution = Solution()
    solution.execute()
