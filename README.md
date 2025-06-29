# TP3---Hadoop

# 1. Set environment variables (Hadoop config and PATH)
$env:HADOOP_CONF_DIR = "C:\hadoop-3.3.6\etc\hadoop"
$env:Path += ";C:\hadoop-3.3.6\bin;C:\hadoop-3.3.6\sbin"

# 2 Start Hadoop DFS and YARN
cd C:\hadoop-3.3.6\sbin
start-dfs.cmd
start-yarn.cmd

# 3 (Optional) Recreate spark-libs.jar correctly if not done already
cd C:\Users\eleah\spark-3.5.5-bin-hadoop3
jar cv0f spark-libs.jar -C jars . -C python lib


# 4 Create HDFS folder (if needed) and upload jar
& "C:\hadoop-3.3.6\bin\hdfs.cmd" dfs -mkdir -p /spark-jars
& "C:\hadoop-3.3.6\bin\hdfs.cmd" dfs -put -f spark-libs.jar /spark-jars/

& "C:\hadoop-3.3.6\bin\hdfs.cmd" dfs -mkdir -p /user/eleah/data
& "C:\hadoop-3.3.6\bin\hdfs.cmd" dfs -put -f data/US_Accidents_March23.csv /user/eleah/data/


# 5. Run your Spark job (Feeder)
cd C:\Users\eleah\OneDrive\Desktop\Cours\BigDataFramework2\TP3-Hadoop
spark-submit --master yarn --deploy-mode client --executor-memory 4G --num-executors 2 --executor-cores 2 --conf "spark.yarn.jars=file:///C:/Users/eleah/spark-3.5.5-bin-hadoop3/jars/*.jar" feeder/feeder.py


# 6. Get the parquets from your local hdfs
& "C:\hadoop-3.3.6\bin\hdfs.cmd" dfs -get /user/eleah/bronze bronze_local


# 7. 

# Hive configuration
$env:Path = "C:\apache-hive-4.0.1-bin\bin;" + $env:Path

# … . Turn on hive
spark-submit  --master yarn --deploy-mode client --executor-memory 4G --num-executors 2 --executor-cores 2 --conf spark.yarn.archive=hdfs:///spark-jars/spark-libs.jar --conf spark.sql.catalogImplementation=hive preprocessor/preprocessor.py
