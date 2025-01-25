import unittest
import pandas as pd
import sys
import os
from stream_data_preprocessing_module import process_data
class TestDataProcessing(unittest.TestCase):
    def setUp(self):
        # Create sample pollution data with at least 10 rows
        self.pollution_data = pd.DataFrame({
            'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
            'id': range(1, 11),
            'lat': [10] * 10,
            'lon': [20] * 10,
            'aqi': [50, 55, 60, 65, 70, 75, 80, 85, 90, 95],
            'co': [0.3] * 10,
            'no2': [15.2] * 10,
            'o3': [25.4] * 10,
            'so2': [5.1] * 10,
            'pm2_5': [12.5] * 10,
            'pm10': [20.3] * 10,
            'nh3': [0.0] * 10
        })
        
        # Create sample weather data with at least 10 rows
        self.weather_data = pd.DataFrame({
            'date': pd.date_range(start='2023-01-01', periods=10, freq='D'),
            'temperature': [30, 32, 31, 33, 34, 35, 36, 37, 38, 39],
            'humidity': [80, 85, 82, 84, 83, 86, 87, 88, 89, 90]
        })
    
    def test_process_data(self):
        result = process_data(self.pollution_data, self.weather_data)
        print(result.shape)
        
        # Check the shape of the result
        self.assertEqual(result.shape, (1, 10, 13))
        
        # Check if the date field is transformed into numerical format
if __name__ == '__main__':
    unittest.main()
