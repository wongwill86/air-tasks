from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_plugin import DockerWithVariablesOperator

PYCHUNKEDGRAPH_DAG_ID = 'pcg_edgetask_pinky100_v0_scheduler'

DATASET_LOWER_BOUND = [17920, 14848, 0]  # Should read that from the info file...
DATASET_UPPER_BOUND = [61440, 40960, 2176]  # Should read that from the info file...
EDGEBUNDLE_SIZE = [1024, 1024, 1152]

EDGEBUNDLE_CONFIG_STR = ' '.join("""{
    "chunkedgraph": {
        "table_id": "pinky100_v0",
        "instance_id": "pychunkedgraph"
    },
    "mysql": {
        "host": "127.0.0.1",
        "user": "root",
        "db": "relabeling"
    },
    "layers": {
        "agglomeration_path_input": "gs://neuroglancer/pinky100_v0/seg/lost_no-random/bbox1_0",
        "watershed_path_input": "gs://neuroglancer/pinky100_v0/ws/lost_no-random/bbox1_0",
        "watershed_path_output": "gs://neuroglancer/nkem/pinky100_v0/ws/lost_no-random/bbox1_0"
    },
    "regiongraph": {
        "chunksize": [512, 512, 272],
        "regiongraph_path_input": "gs://ranl/basil_v0/basil_full/seg/region_graph",
        "regiongraph_path_output": "gs://nkem/pinky100_v0/region_graph"
    }
}""".split())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'catchup_by_default': False,
    'retries': 7,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    dag_id=PYCHUNKEDGRAPH_DAG_ID,
    default_args=default_args,
    schedule_interval=None
)


def create_edge_task(dag, x, y, z):
    p = "'{}' {}-{}_{}-{}_{}-{}".format(
        EDGEBUNDLE_CONFIG_STR,
        x, x + EDGEBUNDLE_SIZE[0],
        y, y + EDGEBUNDLE_SIZE[1],
        z, z + EDGEBUNDLE_SIZE[2]
    )

    t = DockerWithVariablesOperator(
        ['google-secret.json', 'mysql'],
        mount_point='/secrets',
        task_id='docker_task_%s_%s_%s' % (x, y, z),
        command='python -u /usr/local/pychunkedgraph/pychunkedgraph/edge_gen/edgetask.py %s' % p,
        default_args=default_args,
        image='nkemnitz/pychunkedgraph:latest',
        network_mode='host',
        execution_timeout=timedelta(hours=5),
        dag=dag
    )
    t.template_fields = t.template_fields + ('command',)
    return t


def create_knot_task(dag, g):
    return BashOperator(
        task_id='create_knot_task_%s' % g,
        bash_command='echo "Reached knot %s!"' % g,
        dag=dag)


# Need to process all chunks so that their _neighbors_ don't
# accidentally overlap - can this be done with fewer groups (currently 27)?
groups = dict()
for (i, x) in enumerate(range(
        DATASET_LOWER_BOUND[0], DATASET_UPPER_BOUND[0], EDGEBUNDLE_SIZE[0])):
    i = i % 2
    for (j, y) in enumerate(range(
            DATASET_LOWER_BOUND[1], DATASET_UPPER_BOUND[1], EDGEBUNDLE_SIZE[1])):
        j = j % 2
        for (k, z) in enumerate(range(
                DATASET_LOWER_BOUND[2], DATASET_UPPER_BOUND[2], EDGEBUNDLE_SIZE[2])):
            k = k % 2
            group = '%s_%s_%s' % (i, j, k)
            if group in groups:
                groups[group].append(create_edge_task(dag, x, y, z))
            else:
                groups[group] = [create_edge_task(dag, x, y, z)]

knot_tasks = [create_knot_task(dag, g) for g in groups.keys()]

knot_tasks.insert(0, create_knot_task(dag, 'START'))

for (i, v) in enumerate(groups.values()):
    for t in v:
        t.set_upstream(knot_tasks[i])
        t.set_downstream(knot_tasks[i + 1])
