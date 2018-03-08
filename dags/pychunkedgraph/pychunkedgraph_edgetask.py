from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_plugin import DockerWithVariablesOperator

PYCHUNKEDGRAPH_DAG_ID = 'pychunkedgraph_edgetask_scheduler'

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


def create_edge_task(dag, c, x, y, z):
    p = '{}-{}_{}-{}_{}-{}'.format(
                    x - c['low_overlap'],
                    x + c['chunk_size'][0] + c['high_overlap'],
                    y - c['low_overlap'],
                    y + c['chunk_size'][1] + c['high_overlap'],
                    z - c['low_overlap'],
                    z + c['chunk_size'][2] + c['high_overlap'])
    t = DockerWithVariablesOperator(
        ['google-secret.json', 'mysql'],
        mount_point='/secrets',
        task_id='docker_task_%s_%s_%s' % (x, y, z),
        command='python /usr/local/pychunkedgraph/pychunkedgraph/edge_gen/edgetask.py %s' % p,
        default_args=default_args,
        image='nkemnitz/pychunkedgraph:latest',
        network_mode='host',
        execution_timeout=timedelta(hours=1),
        dag=dag
    )
    t.template_fields = t.template_fields + ('command',)
    return t


def create_knot_task(dag, g):
    return BashOperator(
        task_id='create_knot_task_%s' % g,
        bash_command='echo "Reached knot %s!"' % g,
        dag=dag)


c = {  # Can't move config outside for some reason...
    'dataset': 'pinky40',
    'watershed_path': 'gs://neuroglancer/pinky40_v11/watershed',
    'agglomeration_path': 'gs://neuroglancer/pinky40_v11/watershed_mst_trimmed_sem_remap',
    'regiongraph_path': 'gs://nkem/pinky40_v11/mst_trimmed_sem_remap/oldregiongraph',
    'output_path': 'gs://nkem/pinky40_v11/mst_trimmed_sem_remap/region_graph',
    'chunk_size': [1024, 1024, 1024],  # multiple of 512,512,64
    'low_bound': [10240, 7680, 0],
    'high_bound': [65017, 43717, 1004],  # exclusive 1004
    'low_overlap': 0,
    'high_overlap': 1,
}

# Need to process all chunks so that their _neighbors_ don't
# accidentally overlap - can this be done with fewer groups (currently 27)?
groups = dict()
for (i, x) in enumerate(range(
        c['low_bound'][0], c['high_bound'][0], c['chunk_size'][0])):
    i = i % 2
    for (j, y) in enumerate(range(
            c['low_bound'][1], c['high_bound'][1], c['chunk_size'][1])):
        j = j % 2
        for (k, z) in enumerate(range(
                c['low_bound'][2], c['high_bound'][2], c['chunk_size'][2])):
            k = k % 2
            group = '%s_%s_%s' % (i, j, k)
            if group in groups:
                groups[group].append(create_edge_task(dag, c, x, y, z))
            else:
                groups[group] = [create_edge_task(dag, c, x, y, z)]

knot_tasks = [create_knot_task(dag, g) for g in groups.keys()]

knot_tasks.insert(0, create_knot_task(dag, 'START'))

for (i, v) in enumerate(groups.values()):
    for t in v:
        t.set_upstream(knot_tasks[i])
        t.set_downstream(knot_tasks[i + 1])
