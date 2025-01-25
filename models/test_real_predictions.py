import unittest
from tensorflow.keras.models import load_model
from sklearn.preprocessing import MinMaxScaler
import numpy as np
import pandas as pd
import logging
import sys
import os
from datetime import datetime, timedelta
import random
sys.path.append(os.path.join(os.path.dirname(__file__), '../procesing'))
from stream_data_preprocessing_module import process_data


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='/tmp/application.log', filemode='a')
logger = logging.getLogger()

class TestRealPredictions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.model = load_model('models/lstm_model.keras')  # Update with the correct path to your model
            logger.info("LSTM model loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")
            raise

    def generate_random_data(self, num_records=10):
        weather_data = []
        pollution_data = []
        for i in range(num_records):
            date = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
            weather_record = {
                'date': date,
                'temperature': random.uniform(-10, 40),
                'humidity': random.uniform(0, 100)
            }
            pollution_record = {
                'date': date,
                'id': i + 1,
                'lat': random.uniform(-90, 90),
                'lon': random.uniform(-180, 180),
                'aqi': random.randint(0, 500),
                'co': random.uniform(0, 10),
                'no2': random.uniform(0, 10),
                'o3': random.uniform(0, 10),
                'so2': random.uniform(0, 10),
                'pm2_5': random.uniform(0, 10),
                'pm10': random.uniform(0, 10),
                'nh3': random.uniform(0, 10)
            }
            weather_data.append(weather_record)
            pollution_data.append(pollution_record)
        
        weather_df = pd.DataFrame(weather_data)
        pollution_df = pd.DataFrame(pollution_data)
        return weather_df, pollution_df

    def preprocess_and_predict(self, weather_data, pollution_data):
        # Use the process_data function to preprocess the data
        sequence = process_data(pollution_data, weather_data)
        
        # Fit the MinMaxScaler
        scaler = MinMaxScaler()
        scaler.fit(sequence.reshape(-1, sequence.shape[-1]))
        
        # Make prediction using the LSTM model
        try:
            prediction = self.model.predict(sequence)
            logger.info(f"Prediction: {prediction[0]}")
        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            print(e)
            return "Prediction failed"
        
        return prediction[0]  # Return the first prediction (you can adjust this based on your model's output)

    def test_real_prediction(self):
        weather_data, pollution_data = self.generate_random_data(num_records=10)
        prediction = self.preprocess_and_predict(weather_data, pollution_data)
        print(prediction)
        self.assertIsNotNone(prediction)
        logger.info(f"Test prediction: {prediction}")

if __name__ == '__main__':
    unittest.main()

