from datetime import datetime, timedelta
import random
import pandas as pd
from store_weather_data import insert_weather_data
from store_pollution import insert_pollution_data

def generate_random_weather_data(date):
    return {
        'fecha': date,
        'temperatura': round(random.uniform(-10, 35), 2),
        'humedad': round(random.uniform(20, 100), 2),
        'descripcion': random.choice(['clear-sky', 'partly-cloudy', 'rainy', 'snowy', 'stormy'])
    }

def generate_random_pollution_data(date):
    return {
        'date': date,
        'lat': -33.4489,
        'lon': -70.6693,
        'aqi': int(random.uniform(0, 500)),
        "pollution": {
            'pm10': round(random.uniform(0, 500), 2),
            'pm2_5': round(random.uniform(0, 500), 2),
            'no2': round(random.uniform(0, 200), 2),
            'so2': round(random.uniform(0, 200), 2),
            'o3': round(random.uniform(0, 300), 2),
            'co': round(random.uniform(0, 10), 2),
            'nh3': round(random.uniform(0, 100), 2),
            'no': round(random.uniform(0, 100), 2)
        }
    }

def generate_data_for_last_10_days():
    today = datetime.today().date()
    for i in range(30):
        date = today - timedelta(days=i)
        weather_data = generate_random_weather_data(date)
        pollution_data = generate_random_pollution_data(date)
        insert_weather_data(weather_data)
        insert_pollution_data(pollution_data)
        print(f"Inserted data for date: {date}")

if __name__ == "__main__":
    generate_data_for_last_10_days()
