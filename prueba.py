from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test PySpark") \
    .getOrCreate()

print("PySpark is working!")
