# Load the pre-trained LSTM model (ensure the model is saved in the correct path)
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd
import math
import logging 
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='/tmp/application.log', filemode='a')
logger = logging.getLogger()
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
def preprocess_and_predict():
    
    # Simulate preprocessing and prediction (for testing, use random data)
    sequence = generate_random_data(num_features=13)
    sequence = np.expand_dims(sequence, axis=0)  
    print(sequence.shape)
    # Fit the MinMaxScaler
    scaler = MinMaxScaler()
    scaler.fit(sequence.reshape(-1, sequence.shape[1]))
    
    # Make prediction using the LSTM model
    try:
        prediction = model.predict(sequence)
        prediction.reshape(-1,1)
        prediction = np.full(10,prediction[0][0])
        prediction  =  scaler.inverse_transform(prediction.reshape(-1, 10))
        logger.info(f"Prediction: {prediction[0]}")
        return prediction
    except Exception as e:
        logger.error(f"Prediction failed: {e}")
        print(e)
        return "Prediction failed"

predictions = preprocess_and_predict()
print(predictions[0])