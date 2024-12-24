"""
### DAG which produces to and consumes from a Kafka cluster

This DAG will produce messages consisting of several elements to a Kafka cluster and consume
them.
"""
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
import json
import random

# Add the ingestion directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))

from weather_producer import produce_weather_data

NUMBER_OF_TREATS = 2
KAFKA_TOPIC = 'weather_data'
import requests
import time

def get_weather_data(message):
    spark = SparkSession.builder \
        .appName("KafkaWeatherConsumer") \
        .master("local") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .getOrCreate()

    schema = StructType() \
        .add("temperatura", DoubleType()) \
        .add("humedad", DoubleType()) \
        .add("descripcion", StringType())

    # Leer los datos desde Kafka
    kafka_topic = "weather_data"
    kafka_server = "localhost:9092"
    df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_server) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()
        # Procesar los datos JSON
    weather_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
    query = weather_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    try:
        query.awaitTermination()
    except Exception as e:
        print(f"An error occurred while waiting for the query to terminate: {e}")
        query.stop()

@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_weather():
    produce_weather_data_task = ProduceToTopicOperator(
        task_id="produce_weather_data",
        kafka_config_id="kafka_weather",
        topic=KAFKA_TOPIC,
        producer_function=produce_weather_data,
        producer_function_args=["chile"],
        poll_timeout=10,
    )

    consume_weather_data_task = ConsumeFromTopicOperator(
        task_id="consume_weather_data",
        kafka_config_id="kafka_weather",
        topics=[KAFKA_TOPIC],
        apply_function=get_weather_data,
        poll_timeout=20,
        max_messages=20,
        max_batch_size=20,
    )

    produce_weather_data_task >> consume_weather_data_task


produce_consume_weather()