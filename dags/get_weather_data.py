from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
import sys
import os
from store_weather_data import retrieve_weather_data_by_date
sys.path.append(os.path.join(os.path.dirname(__file__), '../cassandra'))
def get_weather_data(**kwargs):
    fecha = kwargs['execution_date'].date()
    weather_data = retrieve_weather_data_by_date(fecha)
    # Convert date objects to strings
    for data in weather_data:
        if isinstance(data['fecha'], date):
            data['fecha'] = data['fecha'].isoformat()
    return json.dumps(weather_data, default=str)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

dag = DAG(
    'get_weather_data',
    default_args=default_args,
)

get_weather_data_task = PythonOperator(
    task_id='get_weather_data_task',
    provide_context=True,
    python_callable=get_weather_data,
    dag=dag,
)
