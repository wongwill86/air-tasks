from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
from airflow.utils.state import State
from airflow.utils.db import provide_session
from airflow.jobs import BackfillJob
from airflow import models
from sqlalchemy import (or_, not_, and_, func)
from amqp.exceptions import ChannelError

DAG_ID = 'z_manager_cluster_scaler'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 0,
}

MANAGER_QUEUE = u'manager'

def resize_workers(queue_name, size):
    print(queue_name)
    print(size)

@provide_session
def find_queues(session=None):
    TI = models.TaskInstance

    query = (
        session
        .query(TI.queue)
        .distinct(TI.queue)
    )

    queues = query.all()
    print(queues)
    return queues


def scale_function(**kwargs):
    from airflow.executors.celery_executor import app as celery_app

    with celery_app.connection_for_read() as connection:
        # We can monitor more queues here
        for queue in find_queues():
            queue_name = queue[0]
            if queue_name == MANAGER_QUEUE:
                continue

            try:
                _, size, _ = celery_app.amqp.queues[queue_name](
                    connection.default_channel).queue_declare(passive=True)
                resize_workers(queue_name, size)
            except ChannelError as e:
                print('Skipping queue %s because %s' % (queue_name, e.message))
                continue

    return "duhh"


dag = DAG(
    dag_id=DAG_ID,
    schedule_interval='0/10 * * * *',
    default_args=default_args,
)


start = BashOperator(
    task_id='bash_task',
    bash_command='sleep 1; echo "Hello from message #' +
                 '{{ dag_run.conf if dag_run else "NO MESSAGE" }}"',
    default_args=default_args,
    queue="manager",
    dag=dag

)


cluster_scale = PythonOperator(
    task_id='cluster_scale',
    provide_context=True,
    python_callable=scale_function,
    queue="manager",
    dag=dag)

start.set_downstream(cluster_scale)
