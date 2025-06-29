from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("javax.jdo.option.ConnectionURL", "jdbc:derby:metastore_db;create=true") \
        .config("spark.sql.warehouse.dir", "metastore_db/warehouse") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()

# Query the managed table
spark.sql("SELECT * FROM accidents_cleaned LIMIT 10").show()

# Stop the Spark session
spark.stop()
