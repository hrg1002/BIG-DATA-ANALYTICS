from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Función para ejecutar el script Python
def run_kafka_consumer():
    subprocess.run(['python3', 'kafka_consumer.py'], check=True)

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
}

with DAG(
    dag_id='docker_kafka_pipeline',
    default_args=default_args,
    description='DAG que ejecuta Docker Compose y Kafka Consumer',
    schedule_interval=None,  # Ejecución manual
    catchup=False,
) as dag:

    # Tarea 1: Levantar servicios con Docker Compose
    docker_compose_up = BashOperator(
        task_id='docker_compose_up',
        bash_command='docker-compose up -d',
        cwd='../docker-compose.yml',  # Cambia a la ruta de tu archivo docker-compose.yml
    )

    # Tarea 2: Ejecutar el script Kafka Consumer
    run_kafka_task = PythonOperator(
        task_id='run_kafka_consumer',
        python_callable=run_kafka_consumer,
    )

    # Definir dependencias
    docker_compose_up >> run_kafka_task