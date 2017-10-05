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


# Currently infrakit can not scale with more than 1 queue! Issue is that,
# we are unable to scale down to 0 instances in infrakit. Only way is to
# destroy the group. To recreate the group we must commit the json file which
# includes all the group definitions (which means that queue groups that we
# want to have 0 members will automatically be recreated! :( )
templated_resize_command = """
{% set queue_sizes = task_instance.xcom_pull(task_ids=params.task_id) %}
{%
set docker_compose_command='docker-compose -f ' +
    conf.get('core', 'airflow_home') + '/docker/docker-compose-CeleryExecutor.yml' +
    ' up -d --no-recreate --no-deps --no-build --no-color'
%}
{%
set docker_infrakit_command='docker run --rm \
-v /var/run/docker.sock:/var/run/docker.sock -v /infrakit/:/infrakit \
-e INFRAKIT_HOME=/infrakit -e INFRAKIT_PLUGINS_DIR=/infrakit/plugins ${INFRAKIT_IMAGE} infrakit'
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
            {{docker_infrakit_command}} manager commit ${{'{'}}INFRAKIT_GROUPS_URL{{'}'}}
            {{docker_infrakit_command}} group scale swarm-workers-{{queue}} {{size}}
        else
            {{docker_infrakit_command}} group destroy swarm-workers-{{queue}}
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
