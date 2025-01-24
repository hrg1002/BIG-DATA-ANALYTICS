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
import os
tf.config.set_visible_devices([], 'GPU')
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
cluster = Cluster(['172.17.0.1','9042'],auth_provider=auth_provider)
session = cluster.connect("weather_data")

# Query weather data from Cassandra
def get_weather_data(start_date, end_date):
    weather_query = "SELECT temperature,humidity FROM weather "
    df = pd.DataFrame(list(session.execute(weather_query)))
    print(df)
    return df

# Function to preprocess and predict using LSTM
def preprocess_and_predict():
    # Extract weather data from Cassandra
    weather_data = get_weather_data(start_date='2023-02-01', end_date='2024-12-01')
    
    # Normalize the weather data
    scaler = MinMaxScaler()
    normalized_weather_data = scaler.fit_transform(weather_data)
    
    # Create sequences for prediction
    sequence_length = 10  # Ensure this matches the expected input shape
    sequences = []
    for i in range(len(normalized_weather_data) - sequence_length):
        sequences.append(normalized_weather_data[i:i + sequence_length])
    
    sequences = np.array(sequences)
    
    # Ensure the dataset is large enough or use .repeat()
    if len(sequences) == 0:
        logger.error("Not enough data to create sequences for prediction.")
        return "Not enough data"
    
    # Expand the feature dimension to 13 by padding with zeros
    padding = np.zeros((sequences.shape[0], sequences.shape[1], 11))
    sequences = np.concatenate((sequences, padding), axis=2)
    # Reshape sequences to match the expected input shape for the LSTM model
    sequences = sequences.reshape((sequences.shape[0], sequences.shape[1], sequences.shape[2]))
    
    # Make prediction using the LSTM model
    try:
        predictions = model.predict(sequences)
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        print(e)
    
    return str(predictions)  # Return the predictions

print(preprocess_and_predict())