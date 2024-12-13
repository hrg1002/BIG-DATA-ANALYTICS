from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys 
import os
BASE_DIR = os.path.join(os.path.dirname(__file__), '../../ingestion')
WEATHER_PRODUCER_SCRIPT = os.path.join(BASE_DIR, 'weather_producer.py')
WEATHER_CONSUMER_SCRIPT = os.path.join(BASE_DIR, 'weather_consumer.py')
def run_weather_producer():
    os.system(f'python {WEATHER_PRODUCER_SCRIPT}')

def consume_weather_data():
    os.system(f'python {WEATHER_CONSUMER_SCRIPT}')


# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 12),
    'retries': 1,
}

with DAG(
    dag_id='ingestion_pipeline',
    default_args=default_args,
    description='DAG que ejecuta Docker Compose y Kafka Consumer',
    schedule_interval=None,  # Ejecución manual
    catchup=False,
) as dag:

    weather_data_producer = PythonOperator(
        task_id='weather_data_producer',
        python_callable=run_weather_producer,
    )

    weather_data_consumer = PythonOperator(
        task_id='weather_data_consumer',
        python_callable=consume_weather_data,
    )

    weather_data_producer >> weather_data_consumer