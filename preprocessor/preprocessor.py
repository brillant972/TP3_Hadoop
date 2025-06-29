from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os

# Define Spark warehouse location
warehouse_dir = "file:///C:/Users/eleah/OneDrive/Desktop/Cours/BigDataFramework2/TP3-Hadoop/spark_warehouse"

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("PreprocessorModule") \
    .config("spark.sql.catalogImplementation", "in-memory") \
    .config("spark.sql.warehouse.dir", warehouse_dir) \
    .enableHiveSupport() \
    .getOrCreate()

# Log start time
print(f"[{datetime.now()}] Starting preprocessing...")

# Define input path: all subfolders containing Parquet files
input_path = "file:///C:/Users/eleah/OneDrive/Desktop/Cours/BigDataFramework2/TP3-Hadoop/bronze_local/2025/01/*"
print(f"Reading data from: {input_path}")

# Read all Parquet files from nested subfolders
df = spark.read.parquet(input_path)

# Drop rows with nulls in critical columns
critical_columns = ["ID", "Start_Time", "End_Time", "Start_Lat", "Start_Lng", "Severity"]
df_cleaned = df.dropna(subset=critical_columns)

# Save the cleaned DataFrame as a managed table in Spark warehouse
df_cleaned.write.mode("overwrite").saveAsTable("accidents_cleaned")

# Log end time
print(f"[{datetime.now()}] Preprocessing completed. Table 'accidents_cleaned' saved to Spark warehouse.")

# Stop Spark session
spark.stop()
