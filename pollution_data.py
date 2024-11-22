import json
from kafka import KafkaProducer
import requests

# This is the Api Key from OpenWeather which is going to be used
api_key="f4166084224574682a0539ae00285104"

# Base URL of the API of OpenWeather
url="http://api.openweathermap.org/data/2.5/air_pollution"

# This is the Kafka server address
kafka_server = 'localhost:9092'  
kafka_topic = 'air_pollution_data'  # Name of the topic to keep the info

# We set up the Kafka producer 
producer = KafkaProducer(
    bootstrap_servers=[kafka_server],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))


# Function to obtain the pollution data of an specific city
def obtain_pollution_data(lat,lon):
    complete_url = f"{url}?lat={lat}&lon={lon}&appid={api_key}" # ChatGPT
    try:
        # Make a request
        response = requests.get(complete_url)
        data = response.json()

        # Verify if the response is correct
        if "list" in data and len(data["list"]) > 0:
            # Extract the information about the JSON response
            main = data["list"][0]
            aqi=main["main"]["aqi"]
            components = main["components"]
            pollution_description = data["list"][0]["components"]

            # Creation of a dic to send to Kafka
            pollution_message = {"lat": lat,
                "lon": lon,
                "aqi": aqi,
                "pollution": components,
                "description": pollution_description}

            # Print the obtained data
            print(f"Air Quality Index (AQI) at coordinates ({lat}, {lon}): {aqi}")
            print(f"The components of pollution in {lat,lon}: {components}")
            print(f"Description of the pollution: {pollution_description}")
            
            # Send the message to the specified Kafka topic
            producer.send(kafka_topic, pollution_message)
            producer.flush()

        else:
            print("An error occured while obtaining the data from the API. Verify the name of the city or the API Key.")

    except Exception as e:
        print(f"An error occured when trying to obtain the information of the pollution: {e}")

if __name__ == "__main__":
    # Latitude and longitude from Santiago
    lat = -33.4489
    lon = -70.6693
    obtain_pollution_data(lat, lon)