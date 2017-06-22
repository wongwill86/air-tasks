from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'catchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    }

dag = DAG("segmentation", default_args=default_args, schedule_interval=None)

segmentation = DockerOperator(
        task_id='watershed_print_' + str(count_docker_print),
        image='098703261575.dkr.ecr.us-east-1.amazonaws.com/chunkflow',
        command='julia -e \'print("hello from chunkflow docker julia!")\'',
        network_mode='bridge',
        dag=dag)


