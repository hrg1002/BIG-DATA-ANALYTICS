import json 
import pandas as pd
import pyarrow as pa
import logging
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Create a Spark session
def consume_air_pollution_data():
    spark = SparkSession.builder \
        .appName("KafkaAirPollutionConsumer") \
        .master("local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

    kafka_bootstrap_servers = "pkc-619z3.us-east1.gcp.confluent.cloud:9092"
    kafka_sasl_username = "FA23PQUO3YDWVFAD"
    kafka_sasl_password = "vFgJIHt+Twpma2/Xv7jqCXvIoyHSwkYvPYiytyvuW2/RXLiKYFgpPzpBGyd3sr3G"
    kafka_options = {
        "kafka.bootstrap.servers": kafka_bootstrap_servers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f"username='{kafka_sasl_username}' "
            f"password='{kafka_sasl_password}';"
        ),
        "subscribe": "air_pollution_data",
        "startingOffsets": "earliest"
    }

    schema = StructType() \
        .add("lat", DoubleType()) \
        .add("lon", DoubleType()) \
        .add("aqi", IntegerType()) \
        .add("pollution", StructType() \
            .add("co", DoubleType()) \
            .add("no", DoubleType()) \
            .add("no2", DoubleType()) \
            .add("o3", DoubleType()) \
            .add("so2", DoubleType()) \
            .add("pm2_5", DoubleType()) \
            .add("pm10", DoubleType()) \
            .add("nh3", DoubleType())
        )

    def process_batch(batch_df, batch_id):
        first_rows = batch_df.head(5)
        for row in first_rows:
            pollution_data = row.asDict()
            df_new = spark.createDataFrame([pollution_data])
            df_new.write.mode("overwrite").parquet("air_pollution_data.parquet")
            print(pollution_data)

    df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()

    pollution_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")

    query = pollution_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(once=True) \
        .foreachBatch(process_batch) \
        .start()

    try:
        query.awaitTermination()
    except Exception as e:
        print(f"An error occurred while waiting for the query to terminate: {e}")
        query.stop()

