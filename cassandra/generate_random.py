from datetime import datetime, timedelta
import random
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def init_cassandra():
    auth_provider = PlainTextAuthProvider(username='cassandra', password='password')
    cluster = Cluster(['172.17.0.2', '9042'], auth_provider=auth_provider)
    session = cluster.connect()
    return session, cluster

def generate_random_weather_data(num_records=10):
    weather_data = []
    for _ in range(num_records):
        record = {
            'date': (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
            'temperature': random.uniform(-10, 40),
            'humidity': random.uniform(0, 100),
            'description': random.choice(['clear-sky', 'rain', 'snow', 'cloudy'])
        }
        weather_data.append(record)
    return weather_data

def generate_random_pollution_data(num_records=10):
    pollution_data = []
    for _ in range(num_records):
        record = {
            'date': (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
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
    return pollution_data

def insert_weather_data(session, weather_data):
    insert_query = """
        INSERT INTO weather_data.weather (id, date, temperature, humidity, description)
        VALUES (uuid(), %s, %s, %s, %s)
    """
    for data in weather_data:
        session.execute(insert_query, (
            data['date'],
            data['temperature'],
            data['humidity'],
            data['description']
        ))

def insert_pollution_data(session, pollution_data):
    insert_query = """
        INSERT INTO pollution_data.pollution (id, date, lat, lon, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
        VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for data in pollution_data:
        session.execute(insert_query, (
            data['date'],
            data['lat'],
            data['lon'],
            data['aqi'],
            data['pollution']['co'],
            data['pollution']['no'],
            data['pollution']['no2'],
            data['pollution']['o3'],
            data['pollution']['so2'],
            data['pollution']['pm2_5'],
            data['pollution']['pm10'],
            data['pollution']['nh3']
        ))

if __name__ == "__main__":
    session, cluster = init_cassandra()
    
    weather_data = generate_random_weather_data(num_records=10)
    insert_weather_data(session, weather_data)
    
    pollution_data = generate_random_pollution_data(num_records=10)
    insert_pollution_data(session, pollution_data)
    
    cluster.shutdown()
