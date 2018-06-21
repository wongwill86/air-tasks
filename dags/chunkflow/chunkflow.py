from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker_plugin import DockerWithVariablesOperator
from chunkblocks.models import Block


INPUT_IMAGE_SOURCE = 'gs://wwong/sub_pinky40_v11/image'
OUTPUT_IMAGE_SOURCE = 'gs://wwong/sub_pinky40_v11/identity'

# BOUNDS = (slice(10240, 14336), slice(10240, 14336), slice(0, 640))
BOUNDS = (slice(10240, 14208), slice(10240, 14208), slice(0, 620))
TASK_SHAPE = (1024, 1024, 128)
OVERLAP = (32, 32, 4)
PATCH_SHAPE = (128, 128, 16)
INFERENCE_FRAMEWORK = 'identity'
BLEND_FAMEWORK = 'average'
MODEL_PATH = 'none'
NET_PATH = 'none'
ACCELERATOR_IDS = 'none'


def spaceless_list(items):
    return str(items).replace(' ', ',')


def get_mod_index(shape):
    return tuple(abs(idx % 3) for idx in shape)


def default_intermediate_name(source_name, mod_index):
    return reduce(lambda x, y: x + '_' + str(y), mod_index, '') + '/'


def create_inference_task(chunk):
    return DockerWithVariablesOperator(
        ['test'],
        mount_point='/test',
        task_id='/inference_' + chunk.unit_indexe,
        command=INFERENCE_COMMAND_TEMPLATE.format(
            task_offset_coordinates=spaceless_list(tuple(s.start for s in chunk.slices))),
        default_args=default_args,
        image='wongwill86/chunkflow:scratch',
        dag=dag
    )


def create_blend_task(count_print_hello):
    return DockerWithVariablesOperator(
        ['test'],
        mount_point='/test',
        task_id='/inference_' + chunk.unit_indexe,
        command=INFERENCE_COMMAND_TEMPLATE.format(
            task_offset_coordinates=spaceless_list(tuple(s.start for s in chunk.slices))),
        default_args=default_args,
        image='wongwill86/chunkflow:scratch',
        dag=dag
    )


INFERENCE_PARAMETERS = {
    'input_image_source': INPUT_IMAGE_SOURCE,
    'output_image_source': OUTPUT_IMAGE_SOURCE,
    'task_shape': spaceless_list(TASK_SHAPE),
    'overlap': spaceless_list(OVERLAP),
    'patch_shape': spaceless_list(PATCH_SHAPE),
    'inference_framework': INFERENCE_FRAMEWORK,
    'blend_framework': BLEND_FAMEWORK,
    'model_path': MODEL_PATH,
    'net_path': NET_PATH,
    'accelerator_ids': ACCELERATOR_IDS,
}

INFERENCE_COMMAND_TEMPLATE = '''
sh -c "chunkflow --input_image_source {input_image_source} \
    --output_image_source {output_image_source} \
    task' + \
    --task_offset_coordinates {{task_offset_coordinates}} \
    --task_shape {task_shape} \
    --overlap {overlap} \
    --intermediate_protocol file:// \
    inference \
    --patch_shape {patch_shape} \
    --inference_framework {inference_framework} \
    --blend_framework {blend_framework} \
    --model_path {model_path} \
    --net_path {net_path} \
    --accelerator_ids {accelerator_ids} \
"
'''.format(**INFERENCE_PARAMETERS)

BLEND_PARAMETERS = {
    'input_image_source': INPUT_IMAGE_SOURCE,
    'output_image_source': OUTPUT_IMAGE_SOURCE,
    'task_shape': spaceless_list(TASK_SHAPE),
    'overlap': spaceless_list(OVERLAP),
}

BLEND_COMMAND_TEMPLATE = '''
sh -c "chunkflow --input_image_source {input_image_source} \
    --output_image_source {output_image_source} \
    task' + \
    --task_offset_coordinates {{task_offset_coordinates}} \
    --task_shape {task_shape} \
    --overlap {overlap} \
    blend \
"
'''.format(**BLEND_PARAMETERS)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'catchup_by_default': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=2),
    'max_retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    }


block = Block(bounds=BOUNDS, chunk_shape=TASK_SHAPE, overlap=OVERLAP)

dag = DAG(
    "chunkflow_test", default_args=default_args, schedule_interval=None)

inference_tasks = dict()
blend_tasks = dict()
for chunk in block.chunk_iterator():
    print(chunk.unit_index)
    inference_tasks[chunk.unit_index] = create_inference_task(chunk)
    blend_tasks[chunk.unit_index] = create_blend_task(chunk)

for unit_index, blend_task in blend_tasks.items():
    inference_tasks[unit_index].set_downstream(blend_task)
    for neighbor_chunk in block.get_all_neighbors(chunk):
        inference_tasks[neighbor_chunk.unit_index].set_downstream(blend_task)
