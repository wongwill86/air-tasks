from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from pprint import pprint
from airflow.utils.db import provide_session
from airflow.jobs import BackfillJob
from airflow import models
from sqlalchemy import (or_, not_)

DAG_ID = 'z_manager_cluster_scaler'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 0,
}


def resize_workers(queue_name, size):
    print(queue_name)
    print(size)

@provide_session
def find_tasked_queues(session=None):

    TI = models.TaskInstance
    DR = models.DagRun
    DM = models.DagModel

    query = (
        session
        .query(TI)
        .outerjoin(DR,
                   and_(DR.dag_id = TI.dag_id,
                        Dr.execution_date == TI.execution_date))
        .filter(or_(DR.run_id == None,
                    not_(DR.run_id.like(BackfillJob.ID_PREFIX + '%'))))
        .outerjoin(DM, DM.dag_id==TI.dag_id)
        .filter(or_(DM.dag_id == None,
                    not_(DM.is_paused)))
    )

    tasks = query.all()
    print(tasks)


def scale_function(**kwargs):
    pprint(kwargs)
    from airflow.executors.celery_executor import app as celery_app

    find_tasked_queues()

    with celery_app.connection_for_read() as connection:
        # We can monitor more queues here
        queues = ['default']
        for queue in queues:
            _, size, _ = celery_app.amqp.queues[queue](
                connection.default_channel).queue_declare(passive=True)
            resize_workers(queue, size)

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


def print_context(ds, **kwargs):
    pprint(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


cluster_scale = PythonOperator(
    task_id='cluster_scale',
    provide_context=True,
    python_callable=scale_function,
    queue="manager",
    dag=dag)

start.set_downstream(cluster_scale)
