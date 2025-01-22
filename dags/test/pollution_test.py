import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag
import json
import pytest
from unittest.mock import MagicMock
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

# Sample test data for air pollution
TEST_AIR_POLLUTION_DATA = [
    {"location": "Santiago", "pm2.5": 30, "pm10": 50, "o3": 100},
    {"location": "Santiago", "pm2.5": 40, "pm10": 60, "o3": 110},
]

class TestPollutionDAG(unittest.TestCase):

    def setUp(self):
        """Load the DAG for testing."""
        self.dagbag = DagBag()
        self.dag_id = 'produce_consume_pollution'
        self.dag = self.dagbag.get_dag(self.dag_id)
        self.tasks = self.dag.tasks

    def test_dag_loaded(self):
        """Test if the DAG is correctly loaded."""
        self.assertIsNotNone(self.dag, "DAG not found")
        self.assertEqual(self.dag.dag_id, self.dag_id)

    def test_task_count(self):
        """Test that the correct number of tasks is defined."""
        self.assertEqual(len(self.tasks), 3, "The DAG should have exactly 3 tasks")

    def test_task_dependencies(self):
        """Test the dependency between the producer and consumer tasks."""
        produce_task = self.dag.get_task('produce_pollution_data')
        consume_task = self.dag.get_task('consume_pollution_data')

        self.assertIn(produce_task.task_id, consume_task.upstream_task_ids,
                      "The consumer task should depend on the producer task")

    @patch('air_pollution_producer.produce_pollution_data')
    @patch('air_pollution_consumer.consume_air_pollution_data')
    def test_produce_and_consume(self, mock_producer, mock_consumer):
        """Test the producer and consumer functions."""

        # Mock producer and consumer functions
        mock_producer.return_value = [(None, json.dumps({"ciudad": "santiago", "contaminacion": 75, "descripcion": "high pollution"}))]
        mock_consumer.return_value = None

        # Test producer
        produced_messages = list(mock_producer(-33.4489, -70.6693))
        self.assertGreater(len(produced_messages), 0, "The producer function did not produce any messages")
        self.assertIn("ciudad", json.loads(produced_messages[0][1]), "Produced message does not contain 'ciudad'")

        # Test consumer
        messages = [MagicMock(value=msg[1].encode('utf-8')) for msg in produced_messages]
        mock_consumer(messages)
        self.assertIsNone(mock_consumer.return_value, "The consumer function did not run correctly")

if __name__ == '__main__':
    unittest.main()
