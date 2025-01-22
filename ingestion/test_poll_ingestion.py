import unittest
from unittest.mock import patch, Mock
import json
from air_pollution_producer import produce_pollution_data
import logging

logging.basicConfig(level=logging.ERROR)  # Avoid logging messages during tests

class TestAirPollutionProducer(unittest.TestCase):

    @patch('air_pollution_producer.requests.get')
    def test_produce_pollution_data_success(self, mock_get):
        #configure the mock to simulate a successful API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "list": [
                {
                    "main": {"aqi": 3},
                    "components": {
                        "co": 201.94,
                        "no": 0.07,
                        "no2": 0.03,
                        "o3": 68.33,
                        "so2": 0.64,
                        "pm2_5": 5.68,
                        "pm10": 7.12,
                        "nh3": 0.77
                    }
                }
            ]
        }
        mock_get.return_value = mock_response

        #call the function to test
        lat, lon = -33.4489, -70.6693
        generator = produce_pollution_data(lat, lon)
        topic, message = next(generator) 

        # Test that the function built the correct URL
        api_key = "f4166084224574682a0539ae00285104"
        expected_url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={api_key}"
        mock_get.assert_called_once_with(expected_url)

        # Verify the produced message
        expected_message = {
            "lat": lat,
            "lon": lon,
            "aqi": 3,
            "pollution": {
                "co": 201.94,
                "no": 0.07,
                "no2": 0.03,
                "o3": 68.33,
                "so2": 0.64,
                "pm2_5": 5.68,
                "pm10": 7.12,
                "nh3": 0.77
            }
        }
        self.assertEqual(json.loads(message), expected_message)

    @patch('air_pollution_producer.requests.get')
    def test_produce_pollution_data_no_data(self, mock_get):
        # Conffigure the mock to simulate an empty API response
        mock_response = Mock()
        mock_response.json.return_value = {}
        mock_get.return_value = mock_response

        # Call the function to test
        lat, lon = -33.4489, -70.6693
        generator = produce_pollution_data(lat, lon)

        with self.assertRaises(StopIteration):  # Verificar que el generador no produce datos
            next(generator)

if __name__ == "__main__":
    unittest.main()
