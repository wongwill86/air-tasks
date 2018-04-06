import itertools
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

GRID_SIZE = [3, 3, 3]
input_dir = 'gs://neuroglancer/golden_v0/image'
output_dir = 'gs://neuroglancer/golden_v0/affinitymap_jwu/'
exchange_dir = 'gs://jpwu/golden_v0/exchange'
output_dataset_start = (84, 576, 576)
output_block_size = (84, 576, 576)
overlap_str = '4,64,64'
patch_size_str = '32,256,256'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'catchup_by_default': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    }
dag = DAG(
    'inference', default_args=default_args, schedule_interval=None)


def get_task_id(z, y, x):
    global GRID_SIZE
    index = x + y*GRID_SIZE[2] + z*GRID_SIZE[1]*GRID_SIZE[2]
    return index


def create_inference_donate_task(z, y, x):
    global input_dir, exchange_dir, output_dir
    global output_dataset_start, output_block_size, overlap_str
    output_block_start = (ds + bs*c for ds, bs, c in
                          zip(output_dataset_start, output_block_size,
                              (z, y, x)))
    output_block_start_str = str(output_block_start)[1:-1]
    output_block_size_str = str(output_block_size)[1:-1]
    return BashOperator(
        # remove the '[' and ']' in two ends
        task_id='inference-donate_' + str(get_task_id(z, y, x)),
        bash_command=('python ~/workspace/chunkflow/python/chunkflow/worker/'
                      'inference_and_donate.py '
                      '--input_dir {input_dir} '
                      '--exchange_dir {exchange_dir} '
                      '--output_dir {output_dir} '
                      '--output_block_start  {output_block_start} '
                      '--output_block_size {output_block_size} '
                      '--patch_size {patch_size} '
                      '--overlap {overlap} '.format(
                          input_dir=input_dir,
                          exchange_dir=exchange_dir,
                          output_dir=output_dir,
                          output_block_start=output_block_start_str,
                          output_block_size=output_block_size_str,
                          patch_size=patch_size_str,
                          overlap=overlap_str)),
        dag=dag)


def create_receive_blend_task(z, y, x):
    global exchange_dir, output_dir
    global output_dataset_start, output_block_size, overlap_str
    output_block_size_str = str(output_block_size)[1:-1]
    output_block_start = (ds + bs*c for ds, bs, c in
                          zip(output_dataset_start, output_block_size,
                              (z, y, x)))
    output_block_start_str = str(output_block_start)[1:-1]

    task_id = 'receive-blend_' + str(get_task_id(z, y, x))
    print(z, y, x, task_id)
    return BashOperator(
        task_id='receive-blend_' + str(get_task_id(z, y, x)),
        bash_command=('python ~/workspace/chunkflow/python/chunkflow/worker/'
                      'receive_and_donate.py '
                      '--exchange_dir {exchange_dir} '
                      '--output_dir {output_dir} '
                      '--output_block_start  {output_block_start} '
                      '--output_block_size {output_block_size} '
                      '--patch_size {patch_size} '
                      '--overlap {overlap} '.format(
                          exchange_dir=exchange_dir,
                          output_dir=output_dir,
                          output_block_start=output_block_start_str,
                          output_block_size=output_block_size_str,
                          patch_size=patch_size_str,
                          overlap=overlap_str)),
        dag=dag)


begin_task = BashOperator(
    task_id='begin_task',
    bash_command='echo "Start here"',
    dag=dag)

done_task = BashOperator(
    task_id='end_task',
    bash_command='echo "I AM DONE"',
    dag=dag)


# inference and donate tasks
inference_task_list = list()
for z, y, x in itertools.product(range(GRID_SIZE[0]), range(GRID_SIZE[1]),
                                 range(GRID_SIZE[2])):
    task = create_inference_donate_task(z, y, x)
    inference_task_list.append(task)
    task.set_upstream(begin_task)

print('number of inference tasks: {}'.format(len(inference_task_list)))

# receive and blend tasks
for z, y, x in range(GRID_SIZE[0]), range(GRID_SIZE[1]), range(GRID_SIZE[2]):
    task = create_receive_blend_task(z, y, x)
    zlist = [z]
    ylist = [y]
    xlist = [x]
    if z > 0:
        zlist.append(z-1)
    if y > 0:
        ylist.append(y-1)
    if x > 0:
        xlist.append(x-1)
    # scan the neighboring blocks
    for zn, yn, xn in itertools.product(zlist, ylist, xlist):
        if zn != z or yn != y or xn != x:
            # this is a neighboring block
            task_index = get_task_id(zn, yn, xn)
            task.set_upstream(inference_task_list[task_index])
            task.set_downstream(done_task)
