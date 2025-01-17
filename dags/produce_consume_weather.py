### DAG which produces to and consumes from a Kafka cluster
import pandas as pd
import sys
import os
from airflow.decorators import dag
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.operators.python_operator import PythonOperator

# Add the ingestion directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))
sys.path.append(os.path.join(os.path.dirname(__file__), '../cassandra'))

from weather_producer import produce_weather_data
from weather_consumer import get_weather_data 
from store_weather_data import insert_weather_data, init

NUMBER_OF_TREATS = 2
KAFKA_TOPIC = 'weather_data'

def store_data():
    path = "./weather_data.parquet"
    weather_data = pd.read_parquet(path,columns = ["temperatura","humedad","descripcion"])
    weather_data['fecha'] = pd.Timestamp.today().date()
    weather_dict = weather_data.iloc[0].to_dict()
    print(weather_dict)
    init()
    insert_weather_data(weather_dict)

@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_weather():
    produce_weather_data_task = ProduceToTopicOperator(
        task_id="produce_weather_data",
        kafka_config_id="kafka_weather",
        topic=KAFKA_TOPIC,
        producer_function=produce_weather_data,
        producer_function_args=["chile"],
        poll_timeout=10,
    )

    consume_weather_data_task = PythonOperator(
        task_id="consume_weather_data",
        python_callable = get_weather_data
    )

    store_weather_data_task = PythonOperator(
        task_id='store_data',
        python_callable=store_data
    )

    produce_weather_data_task >> consume_weather_data_task >> store_weather_data_task

produce_consume_weather()