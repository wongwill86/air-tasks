# You must have the airflow variable 'your_key' set up to run this Dag!

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator

DAG_ID = 'example_docker_with_variables'

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

start = DockerWithVariablesOperator(
    ['your_key'],
    task_id='docker_task',
    command='sh -c "ls /run/variables &&\
        cat /run/variables/your_key && echo done"',
    default_args=default_args,
    image="alpine:latest",
    dag=dag
)
