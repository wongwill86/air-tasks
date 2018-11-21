import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker_plugin import DockerWithVariablesOperator
from chunkblocks.models import Block
from functools import reduce


INPUT_IMAGE_SOURCE = 'gs://wwong/sub_pinky40_v11/image'
OUTPUT_DESTINATION = 'gs://wwong/sub_pinky40_test_9/output'

OFFSET = (0, 40960, 10240)
OVERLAP = (4, 32, 32)
PATCH_SHAPE = (16, 160, 160)
NUM_PATCHES_PER_TASK = (2, 3, 4)
TASK_SHAPE = tuple((ps - olap) * num + olap for ps, olap, num in zip(PATCH_SHAPE, OVERLAP, NUM_PATCHES_PER_TASK))
DATASET_BLOCK = Block(offset=OFFSET, num_chunks=[3, 3, 3], chunk_shape=TASK_SHAPE, overlap=OVERLAP)

INFERENCE_FRAMEWORK = 'pytorch'
BLEND_FAMEWORK = 'average'
MODEL_PATH = 'gs://wwong-net/some/dataset/layer/mito0.py'
checkpoint_path = 'gs://wwong-net/some/dataset/layer/mito0_220k.chkpt'
ACCELERATOR_IDS = '[]'


def underscore_list(items):
    return reduce(lambda x, y: x + '_' + str(y), items, '')


def spaceless_list(items):
    return str(list(items)).replace(' ', '')


def get_mod_index(shape):
    return tuple(abs(idx % 3) for idx in shape)


def default_overlap_name(source_name, mod_index):
    return '%%s/' % (source_name, underscore_list(mod_index))


def create_inference_task(chunk):

    task = DockerWithVariablesOperator(
        ['google-secret.json'],
        mount_point='/usr/local/chunkflow/.cloudvolume/secrets',
        task_id='inference_zyx' + underscore_list(chunk.unit_index),
        command=INFERENCE_COMMAND_TEMPLATE.format(
            task_offset_coordinates=spaceless_list(tuple(s.start for s in chunk.slices))),
        default_args=default_args,
        image='wongwill86/chunkflow:scratch',
        environment={'LOCAL_USER_ID': os.getuid()},
        dag=dag
    )
    print(task.command)

    return task


def create_blend_task(count_print_hello):
    return DockerWithVariablesOperator(
        ['google-secret.json'],
        mount_point='/usr/local/chunkflow/.cloudvolume/secrets',
        task_id='blend_zyx' + underscore_list(chunk.unit_index),
        command=BLEND_COMMAND_TEMPLATE.format(
            task_offset_coordinates=spaceless_list(tuple(s.start for s in chunk.slices))),
        default_args=default_args,
        image='wongwill86/chunkflow:scratch',
        environment={'LOCAL_USER_ID': os.getuid()},
        dag=dag
    )


INFERENCE_PARAMETERS = {
    'input_image_source': INPUT_IMAGE_SOURCE,
    'output_destination': OUTPUT_DESTINATION,
    'task_shape': spaceless_list(TASK_SHAPE),
    'overlap': spaceless_list(OVERLAP),
    'patch_shape': spaceless_list(PATCH_SHAPE),
    'inference_framework': INFERENCE_FRAMEWORK,
    'blend_framework': BLEND_FAMEWORK,
    'model_path': MODEL_PATH,
    'checkpoint_path': checkpoint_path,
    'accelerator_ids': ACCELERATOR_IDS,
}

INFERENCE_COMMAND_TEMPLATE = '''
sh -c "chunkflow --input_image_source {input_image_source} \
    --output_destination {output_destination} \
    task \
    --task_offset_coordinates {{task_offset_coordinates}} \
    --task_shape {task_shape} \
    --overlap {overlap} \
    inference \
    --patch_shape {patch_shape} \
    --inference_framework {inference_framework} \
    --blend_framework {blend_framework} \
    --model_path {model_path} \
    --checkpoint_path {checkpoint_path} \
    --accelerator_ids {accelerator_ids} \
"
'''.format(**INFERENCE_PARAMETERS)

BLEND_PARAMETERS = {
    'input_image_source': INPUT_IMAGE_SOURCE,
    'output_destination': OUTPUT_DESTINATION,
    'task_shape': spaceless_list(TASK_SHAPE),
    'overlap': spaceless_list(OVERLAP),
}

BLEND_COMMAND_TEMPLATE = '''
sh -c "chunkflow --input_image_source {input_image_source} \
    --output_destination {output_destination} \
    task \
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

dag = DAG(
    "chunkflow_test", default_args=default_args, schedule_interval=None)

inference_tasks = dict()
blend_tasks = dict()
for chunk in DATASET_BLOCK.chunk_iterator():
    inference_tasks[chunk.unit_index] = create_inference_task(chunk)
    blend_tasks[chunk.unit_index] = create_blend_task(chunk)

for chunk in DATASET_BLOCK.chunk_iterator():
    blend_task = blend_tasks[chunk.unit_index]
    inference_tasks[chunk.unit_index].set_downstream(blend_task)

    for neighbor_chunk in DATASET_BLOCK.get_all_neighbors(chunk):
        inference_tasks[neighbor_chunk.unit_index].set_downstream(blend_task)
