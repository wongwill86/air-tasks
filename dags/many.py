import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator

from airflow.operators import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    }
dag = DAG("many_ws", default_args=default_args, schedule_interval=None)

def create_print_date(dag, count_print_date):
    return BashOperator(
        task_id='print_date_' + str(count_print_date),
        bash_command='date',
        dag=dag)

def create_print_hello(dag, count_print_hello):
    return BashOperator(
        task_id='print_hello_' + str(count_print_hello),
        bash_command='echo "hello world!"',
        dag=dag)

def create_docker_print(dag, count_docker_print):
    return DockerOperator(
        task_id='watershed_print_' + str(count_docker_print),
        image='watershed',
        command='echo "watershed printing!"',
        network_mode='bridge',
        dag=dag)

begin_task = BashOperator(
    task_id='begin_task',
    bash_command='echo "Start here"',
    dag=dag)

width = 5
print_date_tasks = [ create_print_date(dag, i) for i in range(width)]
print_hello_tasks = [ create_print_hello(dag, i) for i in range(width)]
docker_print_tasks = [ create_docker_print(dag, i) for i in range(width)]

done_task = BashOperator(
    task_id='end_task',
    bash_command='echo "I AM DONE"',
    dag=dag)

for print_date_task in print_date_tasks:
    print_date_task.set_upstream(begin_task)

print_hello_tasks[0].set_upstream(print_date_tasks[0])
print_hello_tasks[0].set_downstream(docker_print_tasks[0])
print_hello_tasks[0].set_downstream(docker_print_tasks[1])

for layer_2_index in range(1, width - 1):
    print_hello_tasks[layer_2_index].set_upstream(
            print_date_tasks[layer_2_index - 1])
    print_hello_tasks[layer_2_index].set_upstream(
            print_date_tasks[layer_2_index])

    print_hello_tasks[layer_2_index].set_downstream(
            docker_print_tasks[layer_2_index + 1])
    print_hello_tasks[layer_2_index].set_downstream(
            docker_print_tasks[layer_2_index])

print_hello_tasks[-1].set_upstream(print_date_tasks[-1])
print_hello_tasks[-1].set_upstream(print_date_tasks[-2])
print_hello_tasks[-1].set_downstream(docker_print_tasks[-1])

for docker_print_task in docker_print_tasks:
    docker_print_task.set_downstream(done_task)


