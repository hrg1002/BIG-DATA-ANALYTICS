from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
default_args = {
    'owner': 'marc',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
docker_compose_up = BashOperator(
    task_id='docker_compose_up',
    bash_command='docker compose up',
    dag=dag,
)

run_kafka_consumer = BashOperator(
    task_id='run_kafka_consumer',
    bash_command='python3 ingestion/kafka_consumer.py',
    dag=dag,
)

docker_compose_up >> run_kafka_consumer