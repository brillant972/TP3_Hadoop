from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveTest") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("CREATE DATABASE IF NOT EXISTS tp3_db")
spark.sql("SHOW DATABASES").show()
