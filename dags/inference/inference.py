from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerWithVariablesOperator

GRID_SIZE = (2,5,5)

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

def get_task_id(z,y,x, GRID_SIZE):
    return x + y*GRID_SIZE[2] + z*GRID_SIZE[1]*GRID_SIZE[2]

def create_inference_task(z,y,x, GRID_SIZE):
    task_id = get_task_id(z,y,x, GRID_SIZE)
    return BashOperator(
        task_id= '{},{},{}'.format( z,y,x ),
        bash_command = 'echo task id: {}'.format( task_id ),
        dag=dag )

begin_task = BashOperator(
    task_id='begin_task',
    bash_command='echo "Start here"',
    dag=dag)

done_task = BashOperator(
    task_id='end_task',
    bash_command='echo "I AM DONE"',
    dag=dag)


inference_task_list = []
for z in range( GRID_SIZE[0] ):
    for y in range( GRID_SIZE[1] ):
        for x in range( GRID_SIZE[2] ):
            task = create_inference_task( z,y,x, GRID_SIZE )
            inference_task_list.append( task )

def is_donor(x):
    return x%2 == 0

def is_receiver(x):
    return x%2 > 0

for z in range( GRID_SIZE[0] ):
    for y in range( GRID_SIZE[1] ):
        for x in range( GRID_SIZE[2] ):
            task_id = get_task_id(z,y,x, GRID_SIZE)
            if is_donor(x) and is_donor(y) and is_donor(z):
                inference_task_list[task_id].set_upstream( begin_task )
            elif is_receiver(x) and is_receiver(y) and is_receiver(z):
                inference_task_list[task_id].set_downstream( done_task )

            if is_receiver( x ):
                if x>0:
                    upstream_task = inference_task_list[ \
                                            get_task_id(z,y,x-1, GRID_SIZE) ]
                    inference_task_list[task_id].set_upstream( upstream_task )
                if x<GRID_SIZE[2]-1:
                    upstream_task = inference_task_list[ \
                                            get_task_id(z,y,x+1, GRID_SIZE) ]
                    inference_task_list[task_id].set_upstream( upstream_task )
            if is_receiver( y ):
                if y>0:
                    upstream_task = inference_task_list[ \
                                            get_task_id(z,y-1,x, GRID_SIZE) ]
                    inference_task_list[task_id].set_upstream( upstream_task )
                if y<GRID_SIZE[1]-1:
                    upstream_task = inference_task_list[ \
                                            get_task_id(z,y+1,x, GRID_SIZE) ]
                    inference_task_list[task_id].set_upstream( upstream_task )
            if is_receiver( z ):
                if z>0:
                    upstream_task = inference_task_list[ \
                                            get_task_id(z-1,y,x, GRID_SIZE) ]
                    inference_task_list[task_id].set_upstream( upstream_task )
                if z<GRID_SIZE[0]-1:
                    upstream_task = inference_task_list[ \
                                            get_task_id(z+1,y,x, GRID_SIZE) ]
                    inference_task_list[task_id].set_upstream( upstream_task )



