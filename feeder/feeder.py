import os
from pyspark.sql import SparkSession
from datetime import datetime
import logging

def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

# Initialize Spark
spark = SparkSession.builder \
    .appName("FeederModule") \
    .enableHiveSupport() \
    .getOrCreate()

logger = get_logger("Feeder")

# Paths
csv_path = "data/US_Accidents_March23.csv"
output_base = "bronze/2025/01"

def split_csv(df):
    logger.info("Splitting CSV into 3 parts...")
    splits = df.randomSplit([1.0, 1.0, 1.0], seed=42)
    logger.info("CSV split complete.")
    return splits

def write_partition(day, dfs_to_merge):
    date_path = f"{output_base}/{str(day).zfill(2)}"
    logger.info(f"Writing to {date_path}")
    combined_df = dfs_to_merge[0]
    for df in dfs_to_merge[1:]:
        combined_df = combined_df.union(df)
    combined_df.write.mode("overwrite").parquet(date_path)
    logger.info(f"Finished writing {date_path}")

def main():
    logger.info("Feeder process started.")
    df = spark.read.option("header", "true").csv(csv_path)
    df_parts = split_csv(df)

    write_partition(1, [df_parts[0]])
    write_partition(2, [df_parts[0], df_parts[1]])
    write_partition(3, [df_parts[0], df_parts[1], df_parts[2]])

    logger.info("Feeder process completed.")
    spark.stop()

if __name__ == "__main__":
    main()
