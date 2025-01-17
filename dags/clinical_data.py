from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd 

# Import the library to use Spark
from pyspark.sql import SparkSession
import pandas as pd 
def obtain_clinical_data() :
# To use it later with Spark, we have to create a session (ChatGPT)
# Specify the path
    archivo_excel = "./Atenciones.xlsx"

# Read the Excel file into a Spark DataFrame
# inferSchema is to detect the type of data
    spark = SparkSession.builder \
    .appName("Excel to Spark") \
    .master("local") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.12:0.13.5") \
    .getOrCreate() 

    spark_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("dataAddress", "'Página1_1'!A17") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(archivo_excel)
        # Delete null rows
    spark_df = spark_df.dropna()

    # Delete duplicated rows
    medical_data = spark_df.dropDuplicates()
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
    transposed_data = medical_data.toPandas() 
    transposed_data = transposed_data.transpose()
    print(transposed_data.head())

    spark.stop()  

    # Show the first lines
    spark_df.show()

    # To save the DataFrame in Parquet format for its post process
    spark_df.write.mode('overwrite').parquet("clean_atenciones.parquet")

    # We close the Spark session when done
    spark.stop()


  

# Definir el DAG de Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "process_medical_data_dag",
    default_args=default_args,
    description="Process medical data from Excel and save as Parquet",
    schedule_interval="0 0 * * *",  # Ejecutar diariamente a medianoche
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    # Definir la tarea de Airflow que ejecuta la función de procesamiento
    process_data_task = PythonOperator(
        task_id="process_medical_data_task",
        python_callable=obtain_clinical_data
    )
    