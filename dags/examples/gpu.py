# This will only work on instances that contain the nvidia runtime environment.
# See https://github.com/NVIDIA/nvidia-docker

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerConfigurableOperator

DAG_ID = 'example_nvidia_docker'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

start = DockerConfigurableOperator(
    container_args={'runtime': 'nvidia'},
    task_id='docker_task',
    command='nvidia-smi',
    default_args=default_args,
    image='nvidia/cuda',
    dag=dag
)
