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
from air_pollution_producer import produce_pollution_data
from air_pollution_consumer import consume_air_pollution_data
from store_pollution import insert_pollution_data
NUMBER_OF_TREATS = 2
KAFKA_TOPIC = 'air_pollution_data'

def store_pollution_data():
    path = "./air_pollution_data.parquet"
    pollution_data = pd.read_parquet(path)
    pollution_data['fecha'] = pd.Timestamp.today().date()
    pollution_dict = pollution_data.iloc[0].to_dict()
    print(pollution_dict)
    insert_pollution_data(pollution_dict)
    # Placeholder for storing pollution data logic


@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_pollution():
    produce_pollution_data_task = ProduceToTopicOperator(
        task_id="produce_pollution_data",
        kafka_config_id="kafka_weather",
        topic=KAFKA_TOPIC,
        producer_function= produce_pollution_data,
        producer_function_args=[-33.4489, -70.6693],
        poll_timeout=10,
    )

    consume_pollution_data_task = PythonOperator(
        task_id="consume_pollution_data",
        python_callable=consume_air_pollution_data
    )

    store_pollution_data_task = PythonOperator(
        task_id='store_pollution_data',
        python_callable=store_pollution_data
    )

    produce_pollution_data_task >> consume_pollution_data_task >> store_pollution_data_task

produce_consume_pollution()
