from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, MapType
import json

def consume_air_pollution_data(messages):
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

    # Create an empty DataFrame with the defined schema
    df = spark.createDataFrame([], schema)

    # Process each message
    for message in messages:
        # Decode the message value
        value = json.loads(message.value().decode('utf-8'))

        # Create a DataFrame from the JSON value
        message_df = spark.createDataFrame([value], schema)

        # Union the message DataFrame with the main DataFrame
        df = df.union(message_df)

    # Show the DataFrame
    df.show()

    # Stop the Spark session
    spark.stop()
