from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys 
import os 
import retrieve_weather_data
sys.path.append(os.path.join(os.path.dirname(__file__), '../cassandra'))
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['you@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def retrieve_weather_data():
    return retrieve_weather_data()

with DAG(
    'retrieve_weather_data',
    default_args=default_args,
    description='A DAG to retrieve weather data from Cassandra',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_weather_data = PythonOperator(
        task_id='extract_weather_data',
        python_callable=retrieve_weather_data,
    )

    extract_weather_data