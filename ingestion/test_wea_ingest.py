import unittest
from unittest.mock import patch, Mock
import json
from weather_producer import produce_weather_data  
class TestProduceWeatherData(unittest.TestCase):

    @patch('weather_producer.requests.get')
    def test_produce_weather_data_success(self, mock_get):
        # simulate a successful API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "cod": 200,
            "main": {"temp": 25.5, "humidity": 60},
            "weather": [{"description": "clear sky"}]
        }
        mock_get.return_value = mock_response

        # call the function to test
        city = "Santiago"
        generator = produce_weather_data(city)
        topic, message = next(generator)  

        # verify that the function built the correct URL
        API_KEY = "bd5bb5aa091b27442966e95e26d17da7"
        BASE_URL = "http://api.openweathermap.org/data/2.5/weather?"
        expected_url = f"{BASE_URL}q={city}&appid={API_KEY}&units=metric"
        mock_get.assert_called_once_with(expected_url)

        # verify the produced message
        expected_message = {
            "ciudad": city,
            "temperatura": 25.5,
            "humedad": 60,
            "descripcion": "clear sky"
        }
        self.assertEqual(json.loads(message), expected_message)

    @patch('weather_producer.requests.get')
    def test_produce_weather_data_error(self, mock_get):
        # simulate an error response from the API
        mock_response = Mock()
        mock_response.json.return_value = {"cod": 404, "message": "city not found"}
        mock_get.return_value = mock_response

        # call the function to test
        city = "InvalidCity"
        generator = produce_weather_data(city)

        # verify that the generator does not produce data
        with self.assertRaises(StopIteration):
            next(generator)

    @patch('weather_producer.requests.get')
    def test_produce_weather_data_exception(self, mock_get):
        # simulate a network error
        mock_get.side_effect = Exception("Network error")

        # call the function to test
        city = "Santiago"
        generator = produce_weather_data(city)

        # verify that the generator does not produce data
        with self.assertRaises(StopIteration):
            next(generator)

if __name__ == "__main__":
    unittest.main()
