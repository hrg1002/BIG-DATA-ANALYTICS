from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])  # Replace with your Cassandra node IP
session = cluster.connect()

# Create a keyspace (if not exists)
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS pollution_data
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")

# Use the keyspace
session.set_keyspace('pollution_data')

# Create a table (if not exists)
session.execute("""
    CREATE TABLE IF NOT EXISTS pollution (
        id UUID PRIMARY KEY,
        lat FLOAT,
        lon FLOAT,
        aqi INT,
        co FLOAT,
        no FLOAT,
        no2 FLOAT,
        o3 FLOAT,
        so2 FLOAT,
        pm2_5 FLOAT,
        pm10 FLOAT,
        nh3 FLOAT
    )
""")

# Function to insert data into the table
def insert_pollution_data(session, data):
    query = """
        INSERT INTO pollution (id, lat, lon, aqi, co, no, no2, o3, so2, pm2_5, pm10, nh3)
        VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    session.execute(query, (
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

# Function to retrieve and print pollution data
def retrieve_pollution_data():
    rows = session.execute("SELECT * FROM pollution")
    for row in rows:
        print(f"Lat: {row.lat}, Lon: {row.lon}, AQI: {row.aqi}, CO: {row.co}, NO: {row.no}, NO2: {row.no2}, O3: {row.o3}, SO2: {row.so2}, PM2.5: {row.pm2_5}, PM10: {row.pm10}, NH3: {row.nh3}")

# Example usage
if __name__ == "__main__":
    # Example processed data
    processed_data = [
        {
            'lat': -33.4489,
            'lon': -70.6693,
            'aqi': 3,
            'pollution': {
                'co': 0.3,
                'no': 0.0,
                'no2': 15.2,
                'o3': 25.4,
                'so2': 5.1,
                'pm2_5': 12.5,
                'pm10': 20.3,
                'nh3': 0.0
            }
        },
        # Add more data as needed
    ]

    # Insert the processed data into the table
    for data in processed_data:
        insert_pollution_data(session, data)

    # Retrieve and print the inserted data
    retrieve_pollution_data()

    # Close the connection
    cluster.shutdown()