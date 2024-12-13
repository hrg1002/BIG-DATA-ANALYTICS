from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType
from kafka import KafkaConsumer

# Define the schema for pollution data
pollution_schema = StructType([
  StructField("co", DoubleType(), True),
  StructField("no", DoubleType(), True),
  StructField("no2", DoubleType(), True),
  StructField("o3", DoubleType(), True),
  StructField("so2", DoubleType(), True),
  StructField("pm2_5", DoubleType(), True),
  StructField("pm10", DoubleType(), True),
  StructField("nh3", DoubleType(), True)
])

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PollutionDataConsumer") \
    .master("local") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Read the data from Kafka
kafka_topic = "air_pollution_data"
kafka_server = "localhost:9092"
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_server) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Process the JSON data
pollution_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), pollution_schema).alias("data")) \
    .select("data.*")

# Call the preprocessing function

# Write the processed data to console
query = pollution_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to terminate
query.awaitTermination()
