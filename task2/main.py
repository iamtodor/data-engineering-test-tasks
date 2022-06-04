from typing import List

import numpy as np
from matplotlib import pyplot
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType


class Job:
    def __init__(self) -> None:
        self.spark = SparkSession.builder.master("local[*]").getOrCreate()
        self.udf_peak_hour = F.udf(self.peak_hour, LongType())

    @staticmethod
    def most_popular_zones(taxi_df: DataFrame, taxi_zones_df: DataFrame, location_id_column: str) -> DataFrame:
        """Which zones have the most either pickups or drop offs overall?"""
        assert location_id_column in ["PULocationID", "DOLocationID"]

        most_pickups_df = taxi_df.withColumnRenamed(location_id_column, "LocationID").groupBy("LocationID").count()

        return (
            most_pickups_df.join(taxi_zones_df, ["LocationID"], how="left")
            .select("count", "Zone")
            .orderBy("count", ascending=False)
        )

    @staticmethod
    def peak_taxi_hours(taxi_df: DataFrame, event_type: str) -> DataFrame:
        """What are the peak hours for pickups or drop-offs for taxi?"""
        assert event_type in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

        return (
            taxi_df.select(event_type)
            .withColumn("hour", F.hour(event_type))
            .groupBy("hour")
            .count()
            .orderBy("count", ascending=False)
        )

    @staticmethod
    def trips_distributed_by_length(taxi_df: DataFrame) -> DataFrame:
        """How are the trips distributed by length?"""
        duration_df = (
            taxi_df.select("tpep_pickup_datetime", "tpep_dropoff_datetime")
            .withColumn(
                "duration in secs",
                F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"),
            )
            .select("duration in secs")
            .withColumn("duration in secs rounded float", F.round(F.col("duration in secs") / 100, 1) * 100)
            .withColumn("duration in secs rounded long", F.col("duration in secs rounded float").cast("long"))
            .select("duration in secs rounded long")
            .withColumnRenamed("duration in secs rounded long", "duration in secs")
            .groupBy("duration in secs")
            .count()
            .orderBy("count", ascending=False)
        )
        duration_df.show(25)

        return duration_df

    @staticmethod
    def plot_duration_histogram(duration_df: DataFrame, top_n_trips: int = 25) -> None:
        assert "count" in duration_df.schema.names
        assert "duration in secs" in duration_df.schema.names

        evaluated_duration = duration_df.take(top_n_trips)
        # create a numeric value for every label
        indexes = list(range(len(evaluated_duration)))

        # split words and counts to different lists
        values = [r["count"] for r in evaluated_duration]
        labels = [r["duration in secs"] for r in evaluated_duration]

        # Plotting
        bar_width = 0.35

        pyplot.bar(indexes, values)

        # add labels
        labelidx = [i + bar_width for i in indexes]
        pyplot.xticks(labelidx, labels)
        pyplot.xlabel("rounded secs to tens")
        pyplot.ylabel("count")
        pyplot.title(f"first {top_n_trips} trips distributed by duration, in secs")
        pyplot.show()

    @staticmethod
    def peak_hour(values_list: List[int]) -> int:
        """find peak hour (the most repeated hour in list of hour)"""
        counts = np.bincount(values_list)
        return int(np.argmax(counts))

    def peak_hours_for_trips(self, taxi_df: DataFrame, event_type: str, is_short_trip: bool) -> DataFrame:
        assert event_type in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]

        event_name = event_type.split("_")[-2]
        assert event_name in ["pickup", "dropoff"]

        event_hour_column_name = f"{event_name} hour"

        duration_df = (
            taxi_df.select("tpep_pickup_datetime", "tpep_dropoff_datetime")
            .withColumn(
                "duration in secs",
                F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"),
            )
            .withColumn(event_hour_column_name, F.hour(event_type))
            .select("duration in secs", event_hour_column_name)
            .withColumn("duration in secs rounded float", F.round(F.col("duration in secs") / 100, 1) * 100)
            .withColumn("duration in secs rounded long", F.col("duration in secs rounded float").cast("long"))
            .select("duration in secs rounded long", event_hour_column_name)
            .withColumnRenamed("duration in secs rounded long", "duration in secs")
            .groupBy("duration in secs")
            .agg(self.udf_peak_hour(F.collect_list(event_hour_column_name)).alias(f"{event_name} peak hours"))
            .orderBy("duration in secs", ascending=is_short_trip)
        )

        return duration_df

    def execute(self) -> None:
        taxi_df = self.spark.read.load("data/yellow_taxi_jan_25_2018")
        # taxi_df.printSchema()

        taxi_zones_df = (
            self.spark.read.option("header", "true").option("inferSchema", "true").csv("data/taxi_zones.csv")
        )

        # taxi_zones_df.printSchema()

        most_popular_pickups = self.most_popular_zones(taxi_df, taxi_zones_df, "PULocationID")
        most_popular_pickups.show(5, truncate=False)

        most_popular_drop_offs = self.most_popular_zones(taxi_df, taxi_zones_df, "DOLocationID")
        most_popular_drop_offs.show(5, truncate=False)

        pickup_hours = self.peak_taxi_hours(taxi_df, "tpep_pickup_datetime")
        pickup_hours.show(5, truncate=False)

        drop_off_hours = self.peak_taxi_hours(taxi_df, "tpep_dropoff_datetime")
        drop_off_hours.show(5, truncate=False)

        duration_df = self.trips_distributed_by_length(taxi_df)
        self.plot_duration_histogram(duration_df)

        peak_pickup_hours_for_short_trips_df = self.peak_hours_for_trips(
            taxi_df, "tpep_pickup_datetime", is_short_trip=True
        )
        peak_pickup_hours_for_short_trips_df.show()

        peak_drop_offs_hours_for_short_trips_df = self.peak_hours_for_trips(
            taxi_df, "tpep_dropoff_datetime", is_short_trip=True
        )
        peak_drop_offs_hours_for_short_trips_df.show()

        peak_pickup_hours_for_long_trips_df = self.peak_hours_for_trips(
            taxi_df, "tpep_pickup_datetime", is_short_trip=False
        )
        peak_pickup_hours_for_long_trips_df.show()

        peak_drop_offs_hours_for_long_trips_df = self.peak_hours_for_trips(
            taxi_df, "tpep_dropoff_datetime", is_short_trip=False
        )
        peak_drop_offs_hours_for_long_trips_df.show()


if __name__ == "__main__":
    job = Job()
    job.execute()
