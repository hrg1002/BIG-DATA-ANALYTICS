"""
### DAG which produces to and consumes from a Kafka cluster

This DAG will produce messages consisting of several elements to a Kafka cluster and consume
them.
"""
import sys
import os
from airflow.decorators import dag
from pendulum import datetime
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

# Add the ingestion directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), '../ingestion'))

from weather_producer import produce_weather_data
from weather_consumer import get_weather_data 
NUMBER_OF_TREATS = 2
KAFKA_TOPIC = 'weather_data'

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

    consume_weather_data_task = ConsumeFromTopicOperator(
        task_id="consume_weather_data",
        kafka_config_id="kafka_weather",
        topics=[KAFKA_TOPIC],
        apply_function=get_weather_data,
        poll_timeout=20,
        max_messages=20,
        max_batch_size=20,
    )

    produce_weather_data_task >> consume_weather_data_task


produce_consume_weather()