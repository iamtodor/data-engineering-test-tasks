from pyspark.sql import SparkSession, Window, WindowSpec, DataFrame
from pyspark.sql import functions as F


class Solution:
    def __init__(self) -> None:
        self.spark_session = SparkSession.builder.master("local[*]").appName("task3").getOrCreate()

        self.input_directory = "data"
        self.stock_level = (
            self.spark_session.read.format("csv")
            .option("header", "true")
            .load(f"{self.input_directory}/stock-level.csv")
        )

        self.transactions = (
            self.spark_session.read.format("csv")
            .option("header", "true")
            .load(f"{self.input_directory}/transactions.csv")
        )

    def run(self) -> None:
        transactions = self.extract_date_and_time()

        window = Window.partitionBy(F.col("pos_id"), F.col("day_of_year")).orderBy("timestamp")

        transactions = self.calculate_time_diff_in_transactions(transactions, window)
        average_daily_sales_threshold = self.calculate_average_daily_sales_threshold()

        transactions = transactions.withColumn(
            "balance_lower_than_avg", (average_daily_sales_threshold > F.col("stock_balance")).cast("integer")
        )

        transactions = transactions.withColumn(
            "cumulative_balance_counter", F.sum(F.col("balance_lower_than_avg")).over(window)
        )

        transactions = transactions.drop("secs_between_transactions", "day_of_year", "date")

        transactions = transactions.withColumn(
            "out_of_stocks",
            F.when(
                (F.col("cumulative_balance_counter") > 2) & (F.col("cumulative_mins_between_transactions") > 4 * 60), 1
            ).otherwise(0),
        )

        transactions.sort(F.col("timestamp")).show(100, truncate=False)

    @staticmethod
    def calculate_time_diff_in_transactions(transactions:  DataFrame, window: WindowSpec) -> DataFrame:
        transactions = transactions.withColumn(
            "secs_between_transactions",
            (
                F.col("timestamp").cast("bigint")
                - F.lag(F.col("timestamp").cast("bigint"), 1).over(Window.partitionBy("pos_id").orderBy("timestamp"))
            ).cast("bigint"),
        )

        transactions = transactions.withColumn(
            "mins_between_transactions", F.bround((F.col("secs_between_transactions") / 60), 2)
        )

        transactions = transactions.withColumn(
            "cumulative_mins_between_transactions", F.bround(F.sum(F.col("mins_between_transactions")).over(window), 2)
        )
        return transactions

    def calculate_average_daily_sales_threshold(self) -> float:
        average_daily_sales = self.stock_level.agg(F.avg(F.col("stock_balance"))).collect()
        average_daily_sales_threshold = round(average_daily_sales[0].asDict()["avg(stock_balance)"] * 1.5, 1)
        return average_daily_sales_threshold

    def extract_date_and_time(self) -> DataFrame:
        transactions = self.transactions.withColumn("timestamp", F.to_utc_timestamp(F.col("date"), "Africa/Blantyre"))
        transactions = transactions.withColumn("day_of_year", F.dayofyear(F.col("timestamp"))).sort(F.col("timestamp"))

        return transactions


if __name__ == "__main__":
    solution = Solution()
    solution.run()
