#### for hive
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import sys

# Init Spark with Hive
spark = SparkSession.builder \
    .appName("Preprocessor") \
    .enableHiveSupport() \
    .getOrCreate()

# Read parquet files from HDFS
df = spark.read.parquet("hdfs://localhost:9000/bronze/2025/01/*")

# Clean (exemple: remove nulls in key columns)
df_cleaned = df.dropna(subset=["ID", "Start_Time", "End_Time", "City", "State"])

# Cast types if needed (ex: string to timestamp)
from pyspark.sql.functions import to_timestamp
df_cleaned = df_cleaned.withColumn("Start_Time", to_timestamp("Start_Time")) \
                       .withColumn("End_Time", to_timestamp("End_Time"))

# Write to Hive
df_cleaned.write.mode("overwrite").format("parquet").saveAsTable("accidents.accidents_cleaned")

spark.stop()
