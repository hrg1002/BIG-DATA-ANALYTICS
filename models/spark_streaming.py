import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import socket
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='/tmp/application.log', filemode='a')
logger = logging.getLogger()

# Check Kafka connection
try:
    socket.create_connection(("kafka", 9092))
    logger.info("Kafka is reachable")
except Exception as e:
    logger.error(f"Connection failed: {e}")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("KafkaLSTMProcessing") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()

# Load the pre-trained LSTM model (ensure the model is saved in the correct path)
try:
    model = load_model('models/lstm_model.keras')  # Update with the correct path to your model
    logger.info("LSTM model loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    raise

# Simulating Random Data for Preprocessing (e.g., 10 random features over 100 timesteps)
def generate_random_data(num_timesteps=10, num_features=13):
    logger.info(f"Generating random data with {num_timesteps} timesteps and {num_features} features.")
    data = np.random.rand(num_timesteps, num_features)
    return data

# Function to preprocess and predict using LSTM
def preprocess_and_predict(message):
    logger.info(f"Processing message: {message}")
    
    # Simulate preprocessing and prediction (for testing, use random data)
    sequence = generate_random_data(num_features=13)
    sequence = np.expand_dims(sequence, axis=0)  
    
    # Make prediction using the LSTM model
    try:
        prediction = MinMaxScaler().inverse_transform(model.predict(sequence))
        logger.info(f"Prediction: {prediction[0][0]}")
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        return "Prediction failed"
    
    return str(prediction[0][0])  # Return the first prediction (you can adjust this based on your model's output)

# Read from Kafka topic
logger.info("Starting to read from Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test_topic") \
    .load()

# Convert Kafka value to string
df = df.selectExpr("CAST(value AS STRING) as message")

# Apply preprocessing and prediction on the incoming Kafka data
def process_message(message):
    return preprocess_and_predict(message)

# Register the function as a UDF (User Defined Function)
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

process_message_udf = udf(process_message, StringType())

# Apply UDF to the dataframe
df = df.withColumn("prediction", process_message_udf(col("message")))

# Output the processed data to the console
query = df.select("message", "prediction") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

logger.info("Starting the streaming query...")

# This will ensure that the streaming job continues indefinitely
query.awaitTermination()

