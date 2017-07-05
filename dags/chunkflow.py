from airflow import DAG
from datetime import datetime, timedelta
# logging.basicConfig(level=logging.INFO)
from airflow.operators import DummyOperator
from airflow.operators.chunkflow_plugin import ChunkFlowTasksFileOperator
DAG_NAME = 'will_chunkflow_test'

TASKS_FILENAME = "./dags/chunkflow/tasks.txt"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=default_args,
    schedule_interval=None
)

start = DummyOperator(
    task_id='start',
    default_args=default_args,
    dag=dag
)

chunkflow_subdag = ChunkFlowTasksFileOperator(DAG_NAME, "chunkflow_tasks",
                                              TASKS_FILENAME, dag=dag)

end = DummyOperator(task_id='end', default_args=default_args, dag=dag)

start.set_downstream(chunkflow_subdag)
chunkflow_subdag.set_downstream(end)
