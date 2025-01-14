from cassandra.cluster import Cluster

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
        date DATE,
        temperature FLOAT,
        humidity FLOAT,
        pressure FLOAT
    )
""")

# Function to insert processed weather data
def insert_weather_data(weather_data):
    insert_query = """
        INSERT INTO weather_data.weather (id, city, date, temperature, humidity, pressure)
        VALUES (uuid(), %s, %s, %s, %s, %s)
    """
    session.execute(insert_query, (
        weather_data['city'],
        weather_data['date'],
        weather_data['temperature'],
        weather_data['humidity'],
        weather_data['pressure']
    ))

# Function to retrieve and print weather data
def retrieve_weather_data():
    rows = session.execute("SELECT * FROM weather_data.weather")
    for row in rows:
        print(f"City: {row.city}, Date: {row.date}, Temperature: {row.temperature}, Humidity: {row.humidity}, Pressure: {row.pressure}")

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
    retrieve_weather_data()
    # ...existing code...
