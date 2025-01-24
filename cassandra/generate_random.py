from datetime import datetime, timedelta
import random
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../cassandra'))
from store_weather_data import insert_weather_data
from store_pollution import insert_pollution_data

def generate_random_data(num_records=20):
    weather_data = []
    pollution_data = []
    for _ in range(num_records):
        fecha = (datetime.now() - timedelta(days=random.randint(0, 365))).date()
        record = {
            'fecha': fecha,
            'temperatura': random.uniform(-10, 40),
            'humedad': random.uniform(0, 100),
            'descripcion': random.choice(['clear-sky', 'rain', 'snow', 'cloudy'])
        }
        weather_data.append(record)
        record = {
            'date':fecha,
            'lat': random.uniform(-90, 90),
            'lon': random.uniform(-180, 180),
            'aqi': random.randint(0, 500),
            'pollution': {
                'co': random.uniform(0, 10),
                'no': random.uniform(0, 10),
                'no2': random.uniform(0, 10),
                'o3': random.uniform(0, 10),
                'so2': random.uniform(0, 10),
                'pm2_5': random.uniform(0, 10),
                'pm10': random.uniform(0, 10),
                'nh3': random.uniform(0, 10)
            }
        }  
        pollution_data.append(record)        
    print(pollution_data)
    return weather_data,pollution_data
        
if __name__ == "__main__":
    
    weather_data , pollution_data = generate_random_data(50)
    for i in range(len(weather_data)) :
        insert_weather_data(weather_data[i])
        insert_pollution_data(pollution_data[i])