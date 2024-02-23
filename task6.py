from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("UpdateWithDataField").getOrCreate()

data = [
    (1, "Jo-hn", "2024-02-23T10:30:00"),
    (2, "Al1ice", "2024-02-23T11:45:00"),
    (3, "Bo:b", "2024-02-23T13:15:00"),
    (4, "Ev:a", "2024-02-23T13:15:00"),
    (5, "Mic=hael", "2024-02-23T15:45:00")
]

df = spark.createDataFrame(data, schema=["id", "name", "dt", "prefixes"])

df.show(truncate=False)


def remove_non_ascii(text):
    return ''.join(char for char in text if 65 <= ord(char) <= 90 or 97 <= (ord(char) <= 122))


# def ascii_ignore(x):
#     return x.encode('ascii', 'ignore').decode('ascii')

df_cleaned = df.withColumn("ascii-name", remove_non_ascii(col("name")))

"""
+---+--------+-------------------+------------+
|id |name    |dt                 |cleaned_name|
+---+--------+-------------------+------------+
|1  |Jo-hn   |2024-02-23T10:30:00|John        |
|2  |Al1ice  |2024-02-23T11:45:00|Alice       |
|3  |Bo:b    |2024-02-23T13:15:00|Bob         |
|4  |Ev:a    |2024-02-23T13:15:00|Eva         |
|5  |Mic=hael|2024-02-23T15:45:00|Michael     |
+---+-------+--------------------+------------+
"""

df_deduplicated = df_cleaned.dropDuplicates(["dt"])

"""
+---+--------+-------------------+------------+
|id |name    |dt                 |cleaned_name|
+---+--------+-------------------+------------+
|1  |Jo-hn   |2024-02-23T10:30:00|John        |
|2  |Al1ice  |2024-02-23T11:45:00|Alice       |
|3  |Bo:b    |2024-02-23T13:15:00|Bob         |
|5  |Mic=hael|2024-02-23T15:45:00|Michael     |
+---+-------+-------------------+------------+
"""