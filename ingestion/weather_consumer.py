from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
import data_preprocessing_module as pm
import json

def transform_fields(value):
    value['temperatura'] = float(value['temperatura'])
    value['humedad'] = float(value['humedad'])
    return value

def get_weather_data(message):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("WeatherDataConsumer") \
        .getOrCreate()

    # Define the schema for the weather data
    schema = StructType() \
        .add("ciudad", StringType()) \
        .add("temperatura", DoubleType(), True) \
        .add("humedad", DoubleType(), True) \
        .add("descripcion", StringType(), True)

    # Create an empty DataFrame with the defined schema
    df = spark.createDataFrame([], schema)
    print(message)
    # Process each message
    # Decode the message value
    value = json.loads(message.value().decode('utf-8'))
    print(value)
    # Transform the fields
    value = transform_fields(value)
    # Create a DataFrame from the JSON value
    message_df = spark.createDataFrame([value], schema)

    # Show the DataFrame
    df.show()
    df.write.mode('overwrite').parquet('weather_data.parquet')
    # Stop the Spark session
    spark.stop()

