from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_plugin import DockerWithVariablesOperator

PYCHUNKEDGRAPH_MESHGEN_3_PLUS_DAG_ID = 'pcg_meshtask_pinky100_v0_lvl3plus_scheduler'

DATASET_LOWER_BOUND = [17920, 14848, 0]  # Should read that from the info file...
DATASET_UPPER_BOUND = [61440, 40960, 2176]  # Should read that from the info file...
MESHBUNDLE_SIZE = [2048, 2048, 512]
CHUNKEDGRAPH_CHUNKSIZE = [512, 512, 128] # Should be read from ChunkedGraph

MESHBUNDLE_CONFIG_STR = ' '.join("""{
    "chunkedgraph": {
        "table_id": "pinky100_neo1",
        "instance_id": "pychunkedgraph"
    },
    "meshing": {
        "mip": 2,
        "max_simplification_error": 40,
        "mesh_dir": "mesh_mip_2_err_40_neo1"
    }
}""".split())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 1, 1),
    'catchup_by_default': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    dag_id=PYCHUNKEDGRAPH_MESHGEN_3_PLUS_DAG_ID,
    default_args=default_args,
    schedule_interval=None
)


def create_mesh_task(dag, l, x, y, z):
    p = "'{}' {} {}-{}_{}-{}_{}-{}".format(
        MESHBUNDLE_CONFIG_STR,
        l,
        x, x + MESHBUNDLE_SIZE[0],
        y, y + MESHBUNDLE_SIZE[1],
        z, z + MESHBUNDLE_SIZE[2]
    )

    t = DockerWithVariablesOperator(
        ['google-secret.json'],
        mount_point='/secrets',
        task_id='docker_task_%s_%s_%s_%s' % (l, x, y, z),
        command='python -u /usr/local/pychunkedgraph/pychunkedgraph/meshing/meshgen.py %s' % p,
        default_args=default_args,
        image='nkemnitz/pychunkedgraph:meshgen',
        network_mode='host',
        execution_timeout=timedelta(hours=10), # Adjust once we have a better estimate
        dag=dag
    )
    t.template_fields = t.template_fields + ('command',)
    return t


def create_knot_task(dag, g):
    return BashOperator(
        task_id='create_knot_task_%s' % g,
        bash_command='echo "Reached knot %s!"' % g,
        dag=dag)


mesh_tasks = {}
knot_tasks = [create_knot_task(dag, 'START')]
layer_lower_bound = DATASET_LOWER_BOUND
layer_chunksize = CHUNKEDGRAPH_CHUNKSIZE
for layer in range(3, 5):
    mesh_tasks[layer] = []
    knot_tasks.append(create_knot_task(dag, str(layer)))
    layer_chunksize = [2 ** max(0, layer - 2) * x for x in CHUNKEDGRAPH_CHUNKSIZE]
    layer_lower_bound = [DATASET_LOWER_BOUND[i] // layer_chunksize[i] * layer_chunksize[i] for i in range(3)]

    for x in range(layer_lower_bound[0], DATASET_UPPER_BOUND[0], MESHBUNDLE_SIZE[0]):
        for y in range(layer_lower_bound[1], DATASET_UPPER_BOUND[1], MESHBUNDLE_SIZE[1]):
            for z in range(layer_lower_bound[2], DATASET_UPPER_BOUND[2], MESHBUNDLE_SIZE[2]):
                mesh_tasks[layer].append(create_mesh_task(dag, layer, x, y, z))

for (i, v) in enumerate(mesh_tasks.values()):
    for t in v:
        t.set_upstream(knot_tasks[i])
        t.set_downstream(knot_tasks[i + 1])
