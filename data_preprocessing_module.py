# Description: Transform data from clinical_data.csv to spark dataframe
from pyspark.sql import SparkSession

# Crear una sesión de Spark
spark = SparkSession.builder.appName("Parquet Reader").getOrCreate()

# Leer archivo .parquet
medical_data = spark.read.parquet("clean_atenciones.parquet")
medical_data.columns
# Mostrar los datos
#process data
respiratory_diseases = [
    "IRA Alta (J00-J06)",
    "Influenza (J09-J11)",
    "Neumonía (J12-J18)",
    "Bronquitis/bronquiolitis aguda (J20-J21)",
    "Crisis obstructiva bronquial (J40-J46)",
    "Otra causa respiratoria (J22, J30-J39, J47, J60-J98)",
    "Covid-19, Virus no identificado U07.2",
    "Covid-19, Virus identificado U07.1"
]
filtered_data = medical_data.filter(medical_data["Total de atenciones de urgencia"].isin(respiratory_diseases))
filtered_data.show() 

