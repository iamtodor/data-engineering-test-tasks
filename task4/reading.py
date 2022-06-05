from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from task4.constants import JDBC_URL, POSTGRES_USER, POSTGRES_PASS, POSTGRES_TABLE


class Solution:
    def __init__(self) -> None:
        self.spark = (
            SparkSession.builder.master("local[*]")
            .config("spark.driver.memory", "3g")
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

        result = self.aggregate(data)
        result = self.rename_columns(result)

        self.save_data(result)

    @staticmethod
    def save_data(result: DataFrame) -> None:
        (result.coalesce(1).write.format("csv").option("header", "true").mode("overwrite").save("result.csv"))

    def load_data(self) -> DataFrame:
        data = (
            self.spark.read.format("jdbc")
            .option("url", JDBC_URL)
            .option("driver", "org.postgresql.Driver")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASS)
            .option("dbtable", POSTGRES_TABLE)
            .load()
            .select("timestamp", "user_id", "user_answer", "answered_correctly")
        )
        return data

    @staticmethod
    def aggregate(data: DataFrame) -> DataFrame:
        result = (
            data.groupBy(F.col("user_id"), F.month(F.col("date")))
            .sum("user_answer", "answered_correctly")
            .orderBy(F.col("user_id"), F.col("month(date)"))
        )

        return result

    @staticmethod
    def rename_columns(result: DataFrame) -> DataFrame:
        result = (
            result.withColumnRenamed("month(date)", "month")
            .withColumnRenamed("sum(user_answer)", "user_answer")
            .withColumnRenamed("sum(answered_correctly)", "answered_correctly")
        )
        return result


if __name__ == "__main__":
    solution = Solution()
    solution.execute()
