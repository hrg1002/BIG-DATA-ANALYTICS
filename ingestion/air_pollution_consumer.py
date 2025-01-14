from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, MapType

# Create a Spark session
spark = SparkSession.builder \
    .appName("KafkaPollutionConsumer") \
    .master("local") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Define the schema for the pollution data coming from Kafka
schema = StructType() \
    .add("lat", DoubleType()) \
    .add("lon", DoubleType()) \
    .add("aqi", StringType()) \
    .add("pollution", MapType(StringType(), DoubleType())) \
    .add("description", MapType(StringType(), DoubleType()))  # Adjust based on the actual structure

# Read data from Kafka
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
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Print the data to the console
query = pollution_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# Wait for the query to finish
query.awaitTermination()
