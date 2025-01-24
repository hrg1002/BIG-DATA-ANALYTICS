from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
import numpy as np
from tensorflow.keras.models import load_model
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("PredictMedicalVisits") \
    .master("local[*]") \
    .getOrCreate()

# Connect to Cassandra
auth_provider = PlainTextAuthProvider(username='cassandra', password='password')
cluster = Cluster(['172.17.0.2','9042'],auth_provider=auth_provider)
session = cluster.connect()

# Fetch data from Cassandra
query = "SELECT * FROM medical_visits"  # Replace with your table and query
rows = session.execute(query)

# Convert fetched data to a Spark DataFrame
columns = [desc.name for desc in rows.column_descriptions]
data = [row for row in rows]
df = spark.createDataFrame(data, columns)

# Load the pre-trained model
try:
    model = load_model('models/medical_visits_model.keras')  # Update with the correct path to your model
    logger.info("Model loaded successfully.")
except Exception as e:
    logger.error(f"Failed to load model: {e}")
    raise

# Preprocess the data (example preprocessing, update as needed)
def preprocess_data(df):
    # Assuming the data needs to be scaled and reshaped
    pandas_df = df.toPandas()
    num_features = len(pandas_df.columns)
    data = pandas_df.values.reshape(-1, num_features)
    return data

# Preprocess the data
preprocessed_data = preprocess_data(df)

# Make predictions
predictions = model.predict(preprocessed_data)

# Convert predictions to a Spark DataFrame
predictions_df = spark.createDataFrame(predictions.tolist(), ["prediction"])

# Show predictions
predictions_df.show()

# Optionally, store predictions back to Cassandra or another sink
# Example: Store predictions back to Cassandra
for idx, prediction in enumerate(predictions):
    session.execute(
        """
        INSERT INTO medical_visit_predictions (id, prediction)
        VALUES (%s, %s)
        """,
        (idx, float(prediction))
    )

logger.info("Predictions stored successfully.")