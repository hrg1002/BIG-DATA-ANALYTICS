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

