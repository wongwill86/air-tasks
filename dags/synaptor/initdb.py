from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.docker_plugin import DockerWithVariablesMultiMountOperator  # noqa

DAG_ID = 'initdb'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 7),
    'cactchup_by_default': False,
    'retries': 10,
    'retry_delay': timedelta(minutes=1),
    'retry_exponential_backoff': False,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

# =============
# run-specific args
proc_url = "PROC_FROM_FILE"

# =============

def init_db(dag):

    task_tag = "init_db"

    return DockerWithVariablesMultiMountOperator(
        ["proc_url"],
        mount_points=["/root/proc_url"],
        task_id=task_tag,
        command=(f"init_db {proc_url}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="dbmessenger",
        dag=dag
        )

# Phase 0
init_step = init_db(dag)
