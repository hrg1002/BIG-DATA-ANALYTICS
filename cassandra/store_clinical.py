from cassandra.cluster import Cluster
import uuid
import pandas as pd

# Connect to the Cassandra cluster
cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

# Create a keyspace
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS clinical_data
    WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
""")

# Use the keyspace
session.set_keyspace('clinical_data')

# Create a table to store clinical data
session.execute("""
    CREATE TABLE IF NOT EXISTS patient_records (
        patient_id UUID PRIMARY KEY,
        name text,
        age int,
        diagnosis text,
        treatment text
    )
""")

# Function to insert clinical data
def insert_clinical_data(patient_id, name, age, diagnosis, treatment):
    session.execute("""
        INSERT INTO patient_records (patient_id, name, age, diagnosis, treatment)
        VALUES (%s, %s, %s, %s, %s)
    """, (patient_id, name, age, diagnosis, treatment))

# Read data from Excel file
df = pd.read_excel('/path/to/atenciones.xls')

# Insert data into Cassandra
for index, row in df.iterrows():
    insert_clinical_data(uuid.uuid4(), row['name'], row['age'], row['diagnosis'], row['treatment'])

# Close the connection
cluster.shutdown()