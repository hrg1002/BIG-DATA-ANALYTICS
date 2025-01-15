import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def init() : 
# Connect to the Cassandra cluster
    auth_provider = PlainTextAuthProvider(username='cassandra', password='password')
    cluster = Cluster(['127.0.0.1','9042'],auth_provider=auth_provider)
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

# Function to retrieve and print weather data
def retrieve_weather_data():
    rows = session.execute("SELECT * FROM weather_data.weather")
    for row in rows:
        print(f"City: {row.city}, Temperature: {row.temperature}, Humidity: {row.humidity}, Description: {row.description}")

# Example usage
if __name__ == "__main__":
    # ...existing code...
    processed_weather_data = {
        'city': 'San Francisco',
        'date': '2023-10-01',
        'temperature': 20.5,
        'humidity': 60.0,
        'pressure': 1012.0
    }
    insert_weather_data(processed_weather_data)
    #retrieve_weather_data()
    # ...existing code...
