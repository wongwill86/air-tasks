from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.db import provide_session
from airflow import models
from amqp.exceptions import ChannelError

DAG_ID = 'z_manager_cluster_scaler'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'catchup': False,
    'retries': 0,
}

SCHEDULE_INTERVAL = '* * * * *'

dag = DAG(
    dag_id=DAG_ID,
    schedule_interval=SCHEDULE_INTERVAL,
    default_args=default_args,
)

MANAGER_QUEUE = u'manager'
QUEUE_SIZES_TASK_ID = 'queue_sizes'
RESCALE_SWARM = 'rescale_swarm'

# Use the stack name to determine if we need to use stack or compose
templated_swarm_command = """
    {% set queue_sizes = task_instance.xcom_pull(task_ids=params.task_id) %}
    {% for queue, size in queue_sizes.items() %}
        if [ -z "${{'{'}}STACK_NAME{{'}'}}" ]; then
             docker-compose -f \
                 {{conf.get('core', 'airflow_home')}}\
/docker/docker-compose-CeleryExecutor.yml scale worker-{{queue}}={{size}}
        else
            docker service scale \
${{'{'}}STACK_NAME{{'}'}}_worker-{{queue}}={{size}}
        fi
        echo {{ queue }}, {{ size }}
    {% endfor %}
"""


@provide_session
def find_queues(session=None):
    TI = models.TaskInstance
    query = (
        session
        .query(TI.queue)
        .distinct(TI.queue)
    )
    queues = query.all()
    return queues


def get_queue_sizes():
    from airflow.executors.celery_executor import app as celery_app

    queue_sizes = {}
    with celery_app.connection_for_read() as connection:
        # We can monitor more queues here
        for queue in find_queues():
            queue_name = queue[0]
            if queue_name == MANAGER_QUEUE:
                continue

            try:
                _, size, _ = celery_app.amqp.queues[queue_name](
                    connection.default_channel).queue_declare(passive=True)
                queue_sizes[queue_name] = size
            except ChannelError as e:
                print('No tasks found for %s because %s' %
                      (queue_name, e.message))
                queue_sizes[queue_name] = 0
                continue

    return queue_sizes


latest = LatestOnlyOperator(
    task_id='latest_only',
    queue='manager',
    dag=dag)

queue_sizes_task = PythonOperator(
    task_id=QUEUE_SIZES_TASK_ID,
    python_callable=get_queue_sizes,
    queue="manager",
    dag=dag)

rescale_swarm_task = BashOperator(
    task_id=RESCALE_SWARM,
    bash_command=templated_swarm_command,
    queue="manager",
    params={'task_id': QUEUE_SIZES_TASK_ID},
    dag=dag)

latest.set_downstream(queue_sizes_task)
queue_sizes_task.set_downstream(rescale_swarm_task)
