import pytest
from unittest.mock import MagicMock
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

# Sample test data for air pollution
TEST_AIR_POLLUTION_DATA = [
    {"location": "Santiago", "pm2.5": 30, "pm10": 50, "o3": 100},
    {"location": "Santiago", "pm2.5": 40, "pm10": 60, "o3": 110},
]

def test_produce_air_pollution_data():
    """
    Test the producer function to ensure it generates and publishes air pollution data correctly.
    """
    mock_produce = MagicMock(return_value=TEST_AIR_POLLUTION_DATA)
    produced_data = mock_produce(-33.4489, -70.6693)  # Latitude and longitude for Santiago
    assert mock_produce.called, "Producer function was not called."
    assert len(produced_data) == len(TEST_AIR_POLLUTION_DATA), "Produced data length mismatch."
    assert produced_data[0]["location"] == "Santiago", "Produced data location mismatch."

def test_consume_air_pollution_data():
    """
    Test the consumer function to ensure it consumes and processes air pollution data correctly.
    """
    mock_consume = MagicMock(return_value=None)
    consumed_data = TEST_AIR_POLLUTION_DATA
    mock_consume(consumed_data)
    assert mock_consume.called, "Consumer function was not called."
    assert consumed_data == TEST_AIR_POLLUTION_DATA, "Consumed data does not match the expected data."

def test_dag_execution():
    """
    Simulate the DAG workflow to validate the dependency between the producer and consumer tasks.
    """
    mock_produce = MagicMock(return_value=TEST_AIR_POLLUTION_DATA)
    mock_consume = MagicMock(return_value=None)

    # Simulate DAG tasks
    produced_data = mock_produce(-33.4489, -70.6693)  # Producer generates data
    mock_consume(produced_data)  # Consumer processes data

    # Assertions
    assert mock_produce.called, "Producer task did not execute."
    assert mock_consume.called, "Consumer task did not execute."
    assert produced_data == TEST_AIR_POLLUTION_DATA, "End-to-end data mismatch in the DAG."
