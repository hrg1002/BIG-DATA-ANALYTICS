import json
from cassandra.cluster import Cluster
from weather_producer import produce_weather_data

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
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
        city TEXT,
        temperature FLOAT,
        humidity FLOAT,
        description TEXT
    )
""")

# Function to insert processed weather data
def insert_weather_data(weather_data):
    insert_query = """
        INSERT INTO weather_data.weather (id, city, temperature, humidity, description)
        VALUES (uuid(), %s, %s, %s, %s)
    """
    session.execute(insert_query, (
        weather_data['ciudad'],
        weather_data['temperatura'],
        weather_data['humedad'],
        weather_data['descripcion']
    ))

# Function to retrieve and print weather data
def retrieve_weather_data():
    rows = session.execute("SELECT * FROM weather_data.weather")
    for row in rows:
        print(f"City: {row.city}, Temperature: {row.temperature}, Humidity: {row.humidity}, Description: {row.description}")

# Example usage
if __name__ == "__main__":
    # Fetch weather data for a specific city
    city = 'San Francisco'
    weather_data_generator = produce_weather_data(city)
    
    for weather_data in weather_data_generator:
        weather_data = json.loads(weather_data[1])  # Extract the weather data from the generator
        insert_weather_data(weather_data)
    
    retrieve_weather_data()
    # ...existing code...
