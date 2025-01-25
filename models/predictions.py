import logging
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
    
    # Fit the MinMaxScaler
    scaler = MinMaxScaler()
    sequence_reshaped = sequence.reshape(-1, sequence.shape[-1])
    scaler.fit(sequence_reshaped)
    
    # Scale the sequence
    sequence_scaled = scaler.transform(sequence_reshaped).reshape(sequence.shape)
    
    # Make prediction using the LSTM model
    try:
        prediction = model.predict(sequence_scaled)
        prediction_reshaped = prediction.reshape(-1, 1)
        prediction_reshaped = np.full(10, prediction_reshaped[0][0])
        prediction = scaler.inverse_transform(prediction_reshaped).reshape(prediction.shape)
        logger.info(f"Prediction: {prediction[0]}")
        return prediction
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        print(e)
        return "Prediction failed"
