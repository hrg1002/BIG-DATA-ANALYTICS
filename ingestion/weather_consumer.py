from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
    # Crear una sesión de Spark
def get_weather_data():
    spark = SparkSession.builder \
        .appName("KafkaWeatherConsumer") \
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
    "subscribe": "weather_data",
    "startingOffsets": "earliest"
}

    schema = StructType() \
        .add("temperatura", DoubleType()) \
        .add("humedad", DoubleType()) \
        .add("descripcion", StringType())

    # Leer los datos desde Kafka
    kafka_topic = "weather_data"
    kafka_server = "localhost:9092"
    df = spark.readStream \
            .format("kafka") \
            .options(**kafka_options) \
            .load()
        # Procesar los datos JSON
    weather_df = df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
    query = weather_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .trigger(once = True) \
        .start()

    try:
        query.awaitTermination()
    except Exception as e:
        print(f"An error occurred while waiting for the query to terminate: {e}")
        query.stop()

