# Basic demonstration of how to get nvidia-docker running
# This will only work with nvidida-drivers >= 367.48 see
# https://github.com/NVIDIA/nvidia-docker/wiki/CUDA for compatibility matrix
# This only works if you have set up `apt-get nvidia-docker2`

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerConfigurableOperator

DAG_ID = 'example_docker_runtime_nvidia'

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
    host_args={'runtime': 'nvidia'},
    task_id='docker_task',
    command='nvidia-smi',
    default_args=default_args,
    image="nvidia/cuda:8.0-runtime-ubuntu16.04",
    dag=dag
)
