"""
### DAG which produces to and consumes from a Kafka cluster for air pollution data

This DAG will produce messages consisting of air pollution data to a Kafka cluster and consume
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

from air_pollution_producer import obtain_pollution_data
from air_pollution_consumer import consume_air_pollution_data

KAFKA_TOPIC = 'air_pollution_data'

@dag(
    start_date=datetime(2023, 4, 1),
    schedule=None,
    catchup=False,
    render_template_as_native_obj=True,
)
def produce_consume_air_pollution():
    produce_air_pollution_data_task = ProduceToTopicOperator(
        task_id="produce_air_pollution_data",
        kafka_config_id="kafka_air_pollution",
        topic=KAFKA_TOPIC,
        producer_function=obtain_pollution_data,
        producer_function_args=[-33.4489, -70.6693],  # Coordinates for Santiago
        poll_timeout=10,
    )

    consume_air_pollution_data_task = ConsumeFromTopicOperator(
        task_id="consume_air_pollution_data",
        kafka_config_id="kafka_air_pollution",
        topics=[KAFKA_TOPIC],
        apply_function=consume_air_pollution_data,
        poll_timeout=20,
        max_messages=20,
        max_batch_size=20,
    )

    produce_air_pollution_data_task >> consume_air_pollution_data_task


produce_consume_air_pollution()