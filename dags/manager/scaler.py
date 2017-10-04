from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.db import provide_session
from airflow import models
import requests
import json

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
BRANCH_RESIZE_TASK_ID = 'branch_resize'
RESCALE_TASK_ID = 'rescale_compose'
QUEUE_URL = 'http://rabbitmq:15672/api/queues/%2f/{}'
QUEUE_USERNAME = 'guest'
QUEUE_PASSWORD = 'guest'


templated_resize_command = """
if mount | grep '{{conf.get('core', 'airflow_home')}}/[dags|plugins]' > /dev/null; then
    echo 'Dag folder or plugin folder is mounted! Will not autoscale!'
else
    {% set queue_sizes = task_instance.xcom_pull(task_ids=params.task_id) %}
    echo 'Try to scale {{queue_sizes}}'
    {% if queue_sizes | length %}
    if [ -z "${{'{'}}INFRAKIT_IMAGE{{'}'}}" ]; then
        echo "Scaling local compose"
        docker-compose -f {{conf.get('core', 'airflow_home')}}/docker/docker-compose-CeleryExecutor.yml \
up -d --no-recreate --no-deps --no-build --no-color \
--scale {% for queue, size in queue_sizes.items() %} worker-{{queue}}={{size}} {% endfor %}
    else
        echo "Scaling infrakit"
        {% for queue, size in queue_sizes.items() %}
        docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v /infrakit/:/infrakit \
-e INFRAKIT_HOME=/infrakit -e INFRAKIT_PLUGINS_DIR=/infrakit/plugins ${{'{'}}INFRAKIT_IMAGE{{'}'}} \
infrakit group scale swarm-workers-{{queue}} {{size}}
        {% endfor %}
    fi
    {% endif %}
fi
""" # noqa


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
    queue_sizes = {}
    for queue in find_queues():
        queue_name = queue[0]
        if queue_name == MANAGER_QUEUE:
            continue

        try:
            response = requests.get(QUEUE_URL.format(queue_name),
                                    auth=(QUEUE_USERNAME, QUEUE_PASSWORD))
            stats = json.loads(response.text)
            size = stats['messages_ready'] + stats['messages_unacknowledged']
            queue_sizes[queue_name] = size
        except Exception as e:
            print('No tasks found for %s because %s' % (queue_name, e.message))
            queue_sizes[queue_name] = 0

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

rescale_task = BashOperator(
    task_id=RESCALE_TASK_ID,
    bash_command=templated_resize_command,
    queue="manager",
    params={'task_id': QUEUE_SIZES_TASK_ID},
    dag=dag)

latest.set_downstream(queue_sizes_task)
queue_sizes_task.set_downstream(rescale_task)
