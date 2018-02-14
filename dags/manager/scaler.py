"""
This dag autoscales your cluster. This only works with docker-compose (local)
and Infrakit (swarm).

For Infrakit, the following environment variables must be set:
    - INFRAKIT_IMAGE - what docker image to use for infrakit
    i.e.infrakit/devbundle:latest
    - INFRAKIT_GROUPS_URL - the location of the groups json file that defines
    the groups definition,
    i.e. https://github.com/wongwill86/examples/blob/master/latest/swarm/groups.json
""" # noqa
from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.latest_only_operator import LatestOnlyOperator
from airflow.utils.db import provide_session
from airflow import models
import requests
import json
import logging
logger = logging.root.getChild(__name__)

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


# To use infrakit with > 1 queue, we will have to modify this code to use
# separate groups file for each queue!
templated_resize_command = """
{% set queue_sizes = task_instance.xcom_pull(task_ids=params.task_id) %}
{%
set docker_compose_command='docker-compose -f ' +
    conf.get('core', 'airflow_home') + '/deploy/docker-compose-CeleryExecutor.yml' +
    ' up -d --no-recreate --no-deps --no-build --no-color'
%}
{%
set docker_infrakit_command='docker run --rm \
-v /var/run/docker.sock:/var/run/docker.sock -v /infrakit/:/infrakit \
-e INFRAKIT_HOME=/infrakit -e INFRAKIT_PLUGINS_DIR=/infrakit/plugins \
-e INFRAKIT_HOST=manager-cluster ${INFRAKIT_IMAGE} infrakit'
%}
if mount | grep '{{conf.get('core', 'airflow_home')}}/[dags|plugins]' > /dev/null; then
    echo 'Dag folder or plugin folder is mounted! Will not autoscale!'
else
    echo 'Try to scale {{queue_sizes}}'
    {% if queue_sizes | length %}
    if [ -z "${{'{'}}INFRAKIT_IMAGE{{'}'}}" ]; then
        echo "Scaling local compose"
        {{docker_compose_command}} --scale \
{% for queue, size in queue_sizes.items() %} worker-{{queue}}={{size}} {% endfor %}
    else
        echo "Scaling infrakit"
        {% for queue, size in queue_sizes.items() %}
        if [ {{size}} -gt 0 ]; then
            if ! {{docker_infrakit_command}} group describe workers-{{queue}}; then
                echo 'Recommitting missing group...'
                {{docker_infrakit_command}} manager commit ${{'{'}}INFRAKIT_GROUPS_URL{{'}'}}
            else
                echo 'Group workers-{{queue}} already exists, no need to commit'
            fi
            echo 'Scaling...'
            {{docker_infrakit_command}} group scale workers-{{queue}} {{size}}
        else
            if {{docker_infrakit_command}} group describe workers-{{queue}}; then
                echo 'Destroying Group workers-{{queue}}'
                {{docker_infrakit_command}} group destroy workers-{{queue}}
            else
                echo 'Group workers-{{queue}} already destroyed'
            fi
        fi
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
        except Exception:
            logger.exception('No tasks found for %s', queue_name)
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
