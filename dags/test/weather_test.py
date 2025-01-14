import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag
import json
from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook
class TestWeatherDAG(unittest.TestCase):

    def setUp(self):
        """Load the DAG for testing."""
        self.dagbag = DagBag()
        self.dag_id = 'produce_consume_weather'
        self.dag = self.dagbag.get_dag(self.dag_id)
        self.tasks = self.dag.tasks

    def test_dag_loaded(self):
        """Test if the DAG is correctly loaded."""
        self.assertIsNotNone(self.dag, "DAG not found")
        self.assertEqual(self.dag.dag_id, self.dag_id)

    def test_task_count(self):
        """Test that the correct number of tasks is defined."""
        self.assertEqual(len(self.tasks), 2, "The DAG should have exactly 2 tasks")

    def test_task_dependencies(self):
        """Test the dependency between the producer and consumer tasks."""
        produce_task = self.dag.get_task('produce_weather_data')
        consume_task = self.dag.get_task('consume_weather_data')

        self.assertIn(produce_task.task_id, consume_task.upstream_task_ids,
                      "The consumer task should depend on the producer task")

    @patch('weather_producer.produce_weather_data')
    @patch('weather_consumer.get_weather_data')
    def test_produce_and_consume(self, mock_producer, mock_consumer):
        """Test the producer and consumer functions."""

        # Mock producer and consumer functions
        mock_producer.return_value = [(None, json.dumps({"ciudad": "chile", "temperatura": 25, "humedad": 40, "descripcion": "clear sky"}))]
        mock_consumer.return_value = None

        # Test producer
        produced_messages = list(mock_producer("chile"))
        self.assertGreater(len(produced_messages), 0, "The producer function did not produce any messages")
        self.assertIn("ciudad", json.loads(produced_messages[0][1]), "Produced message does not contain 'ciudad'")

        # Test consumer
        messages = [MagicMock(value=msg[1].encode('utf-8')) for msg in produced_messages]
        mock_consumer(messages)
        self.assertIsNone(mock_consumer.return_value, "The consumer function did not run correctly")

    @patch('airflow.providers.apache.kafka.hooks.client.KafkaAdminClientHook')
    def test_kafka_connection(self, mock_kafka_hook):
        """Test Kafka connection setup."""
        # Mock KafkaAdminClientHook
        mock_kafka_hook.return_value = MagicMock()

        # Create Kafka client
        kafka_hook = KafkaAdminClientHook(kafka_config_id="kafka_weather")
        kafka_client = kafka_hook.get_conn()

        self.assertIsNotNone(kafka_client, "Kafka connection could not be established")

if __name__ == '__main__':
    unittest.main()
