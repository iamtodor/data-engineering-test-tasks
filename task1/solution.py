import numpy as np
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

# os.environ['SPARK_HOME'] = '/usr/local/Cellar/apache-spark/3.2.1/libexec'
# exec(open(os.path.join(os.environ["SPARK_HOME"], 'python/pyspark/shell.py')).read())


class CouncilsJob:
    def __init__(self) -> None:
        self.spark = SparkSession.builder.master("local[*]").appName("EnglandCouncilsJob").getOrCreate()

        self.input_directory = "data"

    def extract_councils(self) -> DataFrame:
        council_dir = "/england_councils"
        district_councils = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(self.input_directory + council_dir + "/district_councils.csv")
        )
        result_district_councils = district_councils.withColumn("council_type", lit("District Council"))

        london_boroughs = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(self.input_directory + council_dir + "/london_boroughs.csv")
        )
        result_london_boroughs = london_boroughs.withColumn("council_type", lit("London Borough"))

        metropolitan_districts = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(self.input_directory + council_dir + "/metropolitan_districts.csv")
        )
        result_metropolitan_districts = metropolitan_districts.withColumn("council_type", lit("Metropolitan District"))

        unitary_authorities = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(self.input_directory + council_dir + "/unitary_authorities.csv")
        )
        result_unitary_authorities = unitary_authorities.withColumn("council_type", lit("Unitary Authority"))

        result_df = (
            result_district_councils.union(result_london_boroughs)
            .union(result_metropolitan_districts)
            .union(result_unitary_authorities)
        )
        # print(result_df.take(5))
        return result_df

    def extract_avg_price(self) -> DataFrame:
        property_avg_price_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(self.input_directory + "/property_avg_price.csv")
        )
        result_df = property_avg_price_df.select(["local_authority", "avg_price_nov_2019"]).withColumnRenamed(
            "local_authority", "council"
        )
        return result_df

    def extract_sales_volume(self) -> DataFrame:
        property_sales_volume_df = (
            self.spark.read.format("csv")
            .option("header", "true")
            .load(self.input_directory + "/property_sales_volume.csv")
        )
        result_df = property_sales_volume_df.select(["local_authority", "sales_volume_sep_2019"]).withColumnRenamed(
            "local_authority", "council"
        )
        # print(result_df.take(5))
        return result_df

    @staticmethod
    def transform(councils_df: DataFrame, avg_price_df: DataFrame, sales_volume_df: DataFrame) -> DataFrame:
        partial_result = councils_df.join(avg_price_df, ["council"], how="left")
        result_df = partial_result.join(sales_volume_df, ["council"], how="left")
        result_df = result_df.fillna(value=np.nan, subset=["avg_price_nov_2019"])
        print((result_df.count(), len(result_df.columns)))
        print(result_df.head(5))

        return result_df

    def execute(self) -> DataFrame:
        return self.transform(self.extract_councils(), self.extract_avg_price(), self.extract_sales_volume())


if __name__ == "__main__":
    council_job = CouncilsJob()
    council_job.execute()
