from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

# Crear una sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaWeatherConsumer") \
    .master("local") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Esquema de los datos recibidos desde Kafka
schema = StructType() \
    .add("ciudad", StringType()) \
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

# Imprimir los datos en consola
query = weather_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
