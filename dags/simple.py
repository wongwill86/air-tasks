from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    }
dag = DAG("simple_ws", default_args=default_args, schedule_interval=None)


t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = DockerOperator(
    task_id='watershed_sleep',
    image='watershed',
    command='/bin/sleep 10',
    network_mode='bridge',
    dag=dag)

t3 = BashOperator(
    task_id='print_hello',
    bash_command='echo "hello world!"',
    dag=dag)

t4 = BashOperator(
    task_id='print_hello_2',
    bash_command='echo "hello world 2!"',
    dag=dag)

t5 = BashOperator(
    task_id='print_goodbye',
    bash_command='echo "goodbye world!"',
    dag=dag)

t1.set_downstream(t2)
t2.set_downstream(t3)
t2.set_downstream(t4)
t3.set_downstream(t5)
t4.set_downstream(t5)
