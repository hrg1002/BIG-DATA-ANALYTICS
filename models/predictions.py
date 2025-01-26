import logging
import math
import numpy as np
import pandas as pd
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../procesing'))
from stream_data_preprocessing_module import process_data

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='/tmp/application.log', filemode='a')
logger = logging.getLogger()

# Load the model
try:
    model = load_model('models/lstm_model.keras')  # Update with the correct path to your model
    logger.info("LSTM model loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    raise

def preprocess_and_predict(weather_data, pollution_data):
    # Use the process_data function to preprocess the data
    sequence = process_data(pollution_data, weather_data)
    # Make prediction using the LSTM model
    try:
        prediction = model.predict(sequence.reshape(-1, 10, 13))
        prediction = prediction.flatten()  # Flatten the prediction array
        logger.info(f"Prediction: {prediction}")
        prediction *= 10000
        return prediction.tolist()  # Convert to list
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        print(e)
        return "Prediction failed"
