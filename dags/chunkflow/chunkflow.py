import os

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker_plugin import DockerWithVariablesOperator
from chunkblocks.models import Block
from functools import reduce

CHUNKFLOW_IMAGE = 'wongwill86/chunkflow:pznet'

INPUT_IMAGE_SOURCE = 'gs://wwong/sub_pinky40_v11/image'
ACCELERATOR_IDS = '[]'

OFFSET = (0, 40960, 10240)
OVERLAP = (4, 32, 32)
NUM_PATCHES = (4, 3, 3)
NUM_TASKS = (3, 3, 3)

INFERENCE_FRAMEWORK = 'identity'
MODEL_PATH = None
CHECKPOINT_PATH = None
PATCH_SHAPE = (16, 160, 160)
OUTPUT_DESTINATION = 'gs://wwong/sub_pinky40_test_identity/output'
# Output created using:
# chunkflow --input_image_source gs://wwong/sub_pinky40_v11/image     --output_destination
#   gs://wwong/sub_pinky40_test_identity/output   --overlap [4,32,32] --patch_shape [16,160,160] --num_patches
#   [4,3,3] cloudvolume  --min_mips 2 create

# INFERENCE_FRAMEWORK = 'pytorch'
# MODEL_PATH = 'gs://wwong-net/some/dataset/layer/mito0.py'
# CHECKPOINT_PATH = 'gs://wwong-net/some/dataset/layer/mito0_220k.chkpt'
# PATCH_SHAPE = (16, 160, 160)
# OUTPUT_DESTINATION = 'gs://wwong/sub_pinky40_test_pytorch/output'
# # Output created using:
# # chunkflow --input_image_source gs://wwong/sub_pinky40_v11/image     --output_destination
# #   gs://wwong/sub_pinky40_test_pytorch/output   --overlap [4,32,32] --patch_shape [16,160,160] --num_patches [4,3,3]
# #   cloudvolume  --min_mips 2 create

# INFERENCE_FRAMEWORK = 'pznet'
# MODEL_PATH = 'gs://wwong-net/some/dataset/layer/pinky100-cores2.tar.gz'
# CHECKPOINT_PATH = None
# PATCH_SHAPE = (20, 256, 256)
# OUTPUT_DESTINATION = 'gs://wwong/sub_pinky40_test_pznet/output'
# # Output created using:
# # chunkflow --input_image_source gs://wwong/sub_pinky40_v11/image     --output_destination
# #   gs://wwong/sub_pinky40_test_pznet/output   --overlap [4,32,32] --patch_shape [20,256,256] --num_patches [4,3,3]
# #   cloudvolume  --min_mips 2 create

BLEND_FAMEWORK = 'average'

TASK_SHAPE = tuple((ps - olap) * num + olap for ps, olap, num in zip(PATCH_SHAPE, OVERLAP, NUM_PATCHES))
DATASET_BLOCK = Block(offset=OFFSET, num_chunks=NUM_TASKS, chunk_shape=TASK_SHAPE, overlap=OVERLAP)


def underscore_list(items):
    return reduce(lambda x, y: x + '_' + str(y), items, '')


def spaceless_list(items):
    return str(list(items)).replace(' ', '')


def get_mod_index(shape):
    return tuple(abs(idx % 3) for idx in shape)


def default_overlap_name(source_name, mod_index):
    return '%%s/' % (source_name, underscore_list(mod_index))


def create_inference_task(chunk):
    return DockerWithVariablesOperator(
        ['google-secret.json'],
        mount_point='/usr/local/chunkflow/.cloudvolume/secrets',
        task_id='inference_zyx' + underscore_list(chunk.unit_index),
        command=INFERENCE_COMMAND_TEMPLATE.format(
            task_offset_coordinates=spaceless_list(tuple(s.start for s in chunk.slices))),
        default_args=default_args,
        image=CHUNKFLOW_IMAGE,
        environment={'LOCAL_USER_ID': os.getuid()},
        dag=dag
    )


def create_blend_task(count_print_hello):
    return DockerWithVariablesOperator(
        ['google-secret.json'],
        mount_point='/usr/local/chunkflow/.cloudvolume/secrets',
        task_id='blend_zyx' + underscore_list(chunk.unit_index),
        command=BLEND_COMMAND_TEMPLATE.format(
            task_offset_coordinates=spaceless_list(tuple(s.start for s in chunk.slices))),
        default_args=default_args,
        image=CHUNKFLOW_IMAGE,
        environment={'LOCAL_USER_ID': os.getuid()},
        dag=dag
    )

BASE_PARAMETERS = {
    'input_image_source': INPUT_IMAGE_SOURCE,
    'output_destination': OUTPUT_DESTINATION,
    'patch_shape': spaceless_list(PATCH_SHAPE),
    'num_patches': spaceless_list(NUM_PATCHES),
    'overlap': spaceless_list(OVERLAP),
}

BASE_COMMAND = '''
sh -c "chunkflow --input_image_source {input_image_source} \
    --output_destination {output_destination} \
    --patch_shape {patch_shape} \
    --num_patches {num_patches} \
    --overlap {overlap} \
    task \
    --task_offset_coordinates {{task_offset_coordinates}} \
'''.format(**BASE_PARAMETERS)

INFERENCE_PARAMETERS = {
    'inference_framework': INFERENCE_FRAMEWORK,
    'blend_framework': BLEND_FAMEWORK,
    'accelerator_ids': ACCELERATOR_IDS,
}

INFERENCE_COMMAND_TEMPLATE = '''
    {base_command} \
    inference \
    --inference_framework {inference_framework} \
    --blend_framework {blend_framework} \
    --accelerator_ids {accelerator_ids} \
'''.format(base_command=BASE_COMMAND, **INFERENCE_PARAMETERS)

if MODEL_PATH is not None:
    INFERENCE_COMMAND_TEMPLATE = ' '.join([INFERENCE_COMMAND_TEMPLATE, '--model_path', MODEL_PATH])
if CHECKPOINT_PATH is not None:
    INFERENCE_COMMAND_TEMPLATE = ' '.join([INFERENCE_COMMAND_TEMPLATE, '--checkpoint_path', CHECKPOINT_PATH])
INFERENCE_COMMAND_TEMPLATE = INFERENCE_COMMAND_TEMPLATE + '"'

BLEND_COMMAND_TEMPLATE = '''
    {base_command} \
    blend \
"
'''.format(base_command=BASE_COMMAND)

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
