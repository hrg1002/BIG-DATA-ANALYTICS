from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

import json
from kafka import KafkaProducer
import requests
import time 

# This is the Api Key from OpenWeather which is going to be used
api_key="f4166084224574682a0539ae00285104"

# Base URL of the API of OpenWeather
url="http://api.openweathermap.org/data/2.5/air_pollution"


# Function to obtain the pollution data of an specific city
def produce_pollution_data(lat,lon):
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
                "pollution": components,}

            # Print the obtained data
            print(f"Air Quality Index (AQI) at coordinates ({lat}, {lon}): {aqi}")
            print(f"The components of pollution in {lat,lon}: {components}")
            
            # Send the message to the specified Kafka topic
            yield (None, json.dumps(pollution_message))

        else:
            print("An error occured while obtaining the data from the API. Verify the name of the city or the API Key.")

    except Exception as e:
        print(f"An error occured when trying to obtain the information of the pollution: {e}")


def run_air_pollution_producer():
    lat = -33.4489
    lon = -70.6693
    while True:
        obtain_pollution_data(lat, lon)
        time.sleep(5)

def consume_air_pollution_data():
    # Placeholder for the consumer logic
    pass

if __name__ == "__main__":
    # Latitude and longitude from Santiago
    lat = -33.4489
    lon = -70.6693
    obtain_pollution_data(lat, lon)