import json
from kafka import KafkaProducer
import requests

# This is the Api Key from OpenWeather which is going to be used
api_key="bd5bb5aa091b27442966e95e26d17da7"

# Base URL of the API of OpenWeather
url="http://api.openweathermap.org/data/2.5/weather?"

# This is the Kafka server address
kafka_server = 'localhost:9092'  
kafka_topic = 'weather_data'  # Name of the topic to keep the info

# We set up the Kafka producer 
producer = KafkaProducer(bootstrap_servers=[kafka_server],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Function to obtain the climate data of an specific city

def obtain_weather_data(city):
    complete_url = f"{url}q={city}&appid={api_key}&units=metric" # The URL for the API request
    try:
        # Make a request
        response = requests.get(complete_url)
        data = response.json()

        # Verify if the response is correct
        if data["cod"] == 200:

            # Extract the information about the JSON response
            main = data["main"]
            temperature = main["temp"]
            humidity = main["humidity"]
            weather_description = data["weather"][0]["description"]

            # We create a dictionary to hold the data
            weather_message = {"ciudad": city,
                "temperatura": temperature,
                "humedad": humidity,
                "descripcion": weather_description}

            # Print the obtained data
            print(f"Temperature in {city}: {temperature} °C")
            print(f"Humidity in {city}: {humidity}%")
            print(f"Description of the weather: {weather_description}")
        
            # Sending the weather data to the Kafka topic
            producer.send(kafka_topic, weather_message)
            producer.flush()

        else:
            print("An error occured while obtaining the data from the API. Verify the name of the city or the API Key.")

    except Exception as e:
     # If something goes wrong during the API request, catch the error

        print(f"An error occured when trying to obtain the information of the weather: {e}")

if __name__ == "__main__":
    obtain_weather_data("Santiago")