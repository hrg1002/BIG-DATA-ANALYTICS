import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import socket
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='/tmp/application.log', filemode='a')
logger = logging.getLogger()

# Load the pre-trained LSTM model (ensure the model is saved in the correct path)
try:
    model = load_model('models/lstm_model.keras')  
    logger.info("LSTM model loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    raise

# Connect to Cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='password')
cluster = Cluster(['172.17.0.2','9042'],auth_provider=auth_provider)
session = cluster.connect()

# Query weather data from Cassandra
def get_weather_data(start_date, end_date):
    weather_query = "SELECT * FROM weather_data WHERE date >= %s AND date <= %s"
    return pd.DataFrame(list(session.execute(weather_query, (start_date, end_date))))

# Function to preprocess and predict using LSTM
def preprocess_and_predict(message):
    logger.info(f"Processing message: {message}")
    
    # Extract weather data from Cassandra
    weather_data = get_weather_data(start_date='2023-02-01', end_date='2024-12-01')
    
    # Normalize the weather data
    scaler = MinMaxScaler()
    normalized_weather_data = scaler.fit_transform(weather_data.drop('date', axis=1))
    
    # Create sequences for prediction
    sequence_length = 10
    sequences = []
    for i in range(len(normalized_weather_data) - sequence_length):
        sequences.append(normalized_weather_data[i:i + sequence_length])
    
    sequences = np.array(sequences)
    
    # Make prediction using the LSTM model
    try:
        predictions = model.predict(sequences)
        predictions_inverse = scaler.inverse_transform(predictions)
        logger.info(f"Predictions: {predictions_inverse}")
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        return "Prediction failed"
    
    return str(predictions_inverse)  # Return the predictions
