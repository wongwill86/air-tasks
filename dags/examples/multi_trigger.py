from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.custom_plugin import MultiTriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator

SCHEDULE_DAG_ID = 'example_multi_trigger_scheduler'
TARGET_DAG_ID = 'example_multi_trigger_target'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

# ####################### SCHEDULER #################################
scheduler_dag = DAG(
    dag_id=SCHEDULE_DAG_ID,
    default_args=default_args,
    schedule_interval=None
)


def param_generator():
    iterable = range(0, 100)
    for i in iterable:
        yield i


operator = MultiTriggerDagRunOperator(
    task_id='trigger_%s' % TARGET_DAG_ID,
    trigger_dag_id=TARGET_DAG_ID,
    params_list=param_generator(),
    default_args=default_args,
    dag=scheduler_dag)

# ####################### TARGET DAG #################################

target_dag = DAG(
    dag_id=TARGET_DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

start = BashOperator(
    task_id='bash_task',
    bash_command='sleep 1; echo "Hello from message #' +
                 '{{ dag_run.conf if dag_run else "NO MESSAGE" }}"',
    default_args=default_args,
    dag=target_dag
)
