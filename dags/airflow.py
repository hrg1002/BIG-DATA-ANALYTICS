from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Agregar rutas de módulos de "ingestion"
BASE_DIR = "/home/hronda/BIG-DATA-ANALYTICS"
sys.path.insert(0, os.path.join(BASE_DIR, "ingestion"))

# Importar funciones desde ingestion
from ingestion.pollution_data import obtain_pollution_data
from ingestion.weather_data import obtain_weather_data
from ingestion.clinical_data import obtain_clinical_data  

# Configuración por defecto del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Creación del DAG
with DAG(
    "daily_data_ingestion",
    default_args=default_args,
    description="Orquestación de adquisición de datos de contaminación, tiempo y clínicos",
    schedule_interval="0 0 * * *",  # Ejecutar cada día a medianoche
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Tareas de adquisición de datos
    fetch_pollution_data = PythonOperator(
        task_id="fetch_pollution_data",
        python_callable=obtain_pollution_data,
        op_args=[-33.4489, -70.6693],  # Latitud y longitud de Santiago
    )

    fetch_weather_data = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=obtain_weather_data,
        op_args=["Santiago"],  # Santiago De Chile
    )

    process_medical_data_task = PythonOperator(
        task_id="process_medical_data_task",
        python_callable=obtain_clinical_data,
        op_args=["Atenciones.xlsx"], 
    )

    fetch_pollution_data >> fetch_weather_data >> process_medical_data_task
