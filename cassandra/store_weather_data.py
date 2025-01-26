from datetime import date
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd ;

def init() : 
# Connect to the Cassandra cluster
    auth_provider = PlainTextAuthProvider(username='cassandra', password='password')
    cluster = Cluster(['172.17.0.2','9042'],auth_provider=auth_provider)
    session = cluster.connect()

    # Create keyspace
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS weather_data
        WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
    """)

    # Create table
    session.execute("""
        CREATE TABLE IF NOT EXISTS weather_data.weather (
            id UUID PRIMARY KEY,
            date Date,
            temperature FLOAT,
            humidity FLOAT,
            description TEXT
        )
    """)
    session.execute("""
        CREATE TABLE IF NOT EXISTS weather_data.daily_weather (
            date Date PRIMARY KEY,
            max_temperature FLOAT,
            min_temperature FLOAT,
            avg_temperature FLOAT
        ) 
    """)
    return session,cluster

    # Function to insert processed weather data
def insert_weather_data(weather_data):
        session,cluster = init()
        insert_query = """
            INSERT INTO weather_data.weather (id, date, temperature, humidity,description)
            VALUES (uuid(), %s, %s, %s, %s)
        """
        session.execute(insert_query, (
            weather_data['fecha'],
            weather_data['temperatura'],
            weather_data['humedad'],
            weather_data['descripcion']
        ))
        print(f"Inserted weather data for date: {weather_data['fecha']}")
        update_daily_weather(weather_data['fecha'])

def update_daily_weather(fecha):
    session, _ = init()
    rows = session.execute("""
        SELECT temperature FROM weather_data.weather WHERE date = %s ALLOW FILTERING
    """, (fecha,))
    temperatures = [row.temperature for row in rows]
    if temperatures:
        max_temp = max(temperatures)
        min_temp = min(temperatures)
        avg_temp = sum(temperatures) / len(temperatures)
        session.execute("""
            INSERT INTO weather_data.daily_weather (date, max_temperature, min_temperature, avg_temperature)
            VALUES (%s, %s, %s, %s)
        """, (fecha, max_temp, min_temp, avg_temp))
        print(f"Updated daily weather for date: {fecha}")

# Function to retrieve and print weather data
def retrieve_weather_data():
    session, _ = init()
    rows = session.execute("SELECT * FROM weather_data.weather")
    for row in rows:
        row.date = row.date.isoformat()
        # Example usage
        print(f"Fecha: {row.date}, Temperature: {row.temperature}, Humidity: {row.humidity}, Description: {row.description}")
        return rows

# Function to retrieve and print daily weather data
def retrieve_daily_weather_data():
    session, _ = init()
    rows = session.execute("SELECT * FROM weather_data.daily_weather")
    for row in rows:
        print(f"Fecha: {row.date}, Max Temperature: {row.max_temperature}, Min Temperature: {row.min_temperature}, Avg Temperature: {row.avg_temperature}")
    return rows

def retrieve_daily_weather_by_date(start_date, end_date):
    session, _ = init()
    rows = session.execute("""
        SELECT * FROM weather_data.daily_weather 
        WHERE date >= %s AND date <= %s
        ALLOW FILTERING 
    """, (start_date, end_date))
    return rows

def retrieve_weather_data_by_date():
    session, _ = init()
    today = date.today()
    rows = session.execute("SELECT * FROM weather_data.weather WHERE date = %s ALLOW FILTERING", (today,))
    weather_data = []
    for row in rows:
        weather_data.append({
            'fecha': str(row.date),
            'temperatura': row.temperature,
            'humedad': row.humidity,
            'descripcion': row.description
        })
    return weather_data

# Example usage
if __name__ == "__main__":
    # ...existing code...
    processed_weather_data = [
        {'fecha': pd.Timestamp.today().date(), 'temperatura': 20, 'humedad': 60, 'descripcion': "clear-sky"},
        {'fecha': pd.Timestamp.today().date(), 'temperatura': 25, 'humedad': 55, 'descripcion': "partly-cloudy"},
        {'fecha': pd.Timestamp.today().date(), 'temperatura': 15, 'humedad': 65, 'descripcion': "rainy"},
    ]
    for data in processed_weather_data:
        insert_weather_data(data)
        retrieve_daily_weather_data()
    start_date = pd.Timestamp('2025-01-25').date()
    end_date = pd.Timestamp('2025-01-25').date()
    retrieve_daily_weather_by_date(start_date, end_date)
    # ...existing code...
