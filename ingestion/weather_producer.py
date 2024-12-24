
import json
import requests
# This is the Api Key from OpenWeather which is going to be used
API_KEY="bd5bb5aa091b27442966e95e26d17da7"

# Base URL of the API of OpenWeather
BASE_URL="http://api.openweathermap.org/data/2.5/weather?"

def produce_weather_data(city):
    complete_url = f"{BASE_URL}q={city}&appid={API_KEY}&units=metric" # The URL for the API request
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
            # Yield a tuple with None as the key and the weather_message as the value
            yield (None, json.dumps(weather_message))
        else:
            print("An error occured while obtaining the data from the API. Verify the name of the city or the API Key.")

    except Exception as e:
     # If something goes wrong during the API request, catch the error

        print(f"An error occured when trying to obtain the information of the weather: {e}")

