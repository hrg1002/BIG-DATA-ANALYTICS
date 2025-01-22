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
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', filename='/tmp/application.log', filemode='a')
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
session_weather = cluster.connect("weather_data")
session_pollution = cluster.connect("pollution_data")

# Query pollution data from Cassandra
def get_pollution_data():
    pollution_query = "SELECT * FROM pollution WHERE date = %s ALLOW FILTERING"
    today = pd.Timestamp.today().date()
    df = pd.DataFrame(list(session_pollution.execute(pollution_query, (today,))))
    print(df)
    if 'id' in df.columns and 'date' in df.columns:
        df = df.drop(['id', 'date'], axis=1)
    return df

# Query weather data from Cassandra
def get_weather_data():
    weather_query = "SELECT temperature,humidity FROM weather WHERE date = %s ALLOW FILTERING"
    today = pd.Timestamp.today().date()
    df = pd.DataFrame(list(session_weather.execute(weather_query, (today,))))
    return df
def generate_random_data(num_timesteps=10, num_features=13):
    logger.info(f"Generating random data with {num_timesteps} timesteps and {num_features} features.")
    data = np.random.rand(num_timesteps, num_features)
    return data
# Function to preprocess and predict using LSTM
def preprocess_and_predict():
    # Extract weather and pollution data from Cassandra
    weather_data = get_weather_data()
    pollution_data = get_pollution_data()
    
    # Merge the data column-wise
    merged_data = pd.concat([weather_data, pollution_data], axis=1)
    sequence = merged_data.head(10).to_numpy()
    print(sequence.shape)
    
    # Ensure the dataset is large enough or use .repeat()
    if len(sequence) == 0:
        logger.error("Not enough data to create sequences for prediction.")
        return "Not enough data"
    
    # Reshape sequences to match the expected input shape for the LSTM model
    sequence = sequence.reshape(-1, 10, merged_data.shape[1])
    print(sequence.shape)
     # Simulate preprocessing and prediction (for testing, use random data)
    # Make prediction using the LSTM model
    try :
        predictions = model.predict(sequence)
        logger.info(f"Predictions: {predictions}")
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        print(e)
        return "Prediction failed"
    
    return predictions  # Return the predictions

print(preprocess_and_predict())