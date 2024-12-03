# Description: Transform data from clinical_data.csv to spark dataframe
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import current_date, month, dayofmonth, expr
from pyspark.sql.types import StructType, StringType, DoubleType

# Crear una sesión de Spark
def process_medical_data() :
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
from datetime import datetime

def process_weather_data(df):
    """
    Add a new column 'season' to the DataFrame based on the current date.
    """
    # Add columns for the current month and day of the month
    df = df.withColumn("current_month", month(current_date())) \
           .withColumn("current_day", dayofmonth(current_date()))
    
    # Use a CASE WHEN expression to determine the season
    df = df.withColumn(
        "season",
        expr("""
            CASE
                WHEN (current_month = 3 AND current_day >= 20) OR (current_month > 3 AND current_month < 6) OR (current_month = 6 AND current_day < 21) THEN 'spring'
                WHEN (current_month = 6 AND current_day >= 21) OR (current_month > 6 AND current_month < 9) OR (current_month = 9 AND current_day < 23) THEN 'summer'
                WHEN (current_month = 9 AND current_day >= 23) OR (current_month > 9 AND current_month < 12) OR (current_month = 12 AND current_day < 21) THEN 'autumn'
                ELSE 'winter'
            END
        """)
    )
    
    # Drop temporary columns if not needed
    df = df.drop("current_month", "current_day")
    
    return df
