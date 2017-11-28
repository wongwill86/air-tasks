from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_plugin import DockerWithVariablesOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 28),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    }
dag = DAG(
    "test_watershed", default_args=default_args, schedule_interval=None)


def atomic_chunk(dag, tag):
    return DockerWithVariablesOperator(
        ['google-secret.json'],
        mount_point='/root/.cloudvolume/secrets/',
        task_id='atomic_chunk_' + tag,
        command='/bin/bash -c "touch /root/.cloudvolume/project_name && ../scripts/atomic_chunk.sh {}.json"'.format(tag),
        default_args=default_args,
        image="ranlu/watershed:0.0.2",
        dag=dag
    )

def composite_chunk(dag, tag):
    return DockerWithVariablesOperator(
        ['google-secret.json'],
        mount_point='/root/.cloudvolume/secrets/',
        task_id='composite_chunk_' + tag,
        command='/bin/bash -c "touch /root/.cloudvolume/project_name && ../scripts/composite_chunk.sh {}.json"'.format(tag),
        default_args=default_args,
        image="ranlu/watershed:0.0.2",
        dag=dag
    )

chunks = {}
chunks["2_0_0_0"]=composite_chunk(dag, "2_0_0_0")
chunks["1_0_0_1"]=composite_chunk(dag, "1_0_0_1")
chunks["1_0_0_0"]=composite_chunk(dag, "1_0_0_0")
chunks["1_0_1_0"]=composite_chunk(dag, "1_0_1_0")
chunks["1_0_1_1"]=composite_chunk(dag, "1_0_1_1")
chunks["1_1_1_1"]=composite_chunk(dag, "1_1_1_1")
chunks["1_1_0_0"]=composite_chunk(dag, "1_1_0_0")
chunks["1_1_0_1"]=composite_chunk(dag, "1_1_0_1")
chunks["1_1_1_0"]=composite_chunk(dag, "1_1_1_0")
chunks["0_0_0_3"]=atomic_chunk(dag, "0_0_0_3")
chunks["0_0_0_2"]=atomic_chunk(dag, "0_0_0_2")
chunks["0_0_1_2"]=atomic_chunk(dag, "0_0_1_2")
chunks["0_0_1_3"]=atomic_chunk(dag, "0_0_1_3")
chunks["0_1_1_3"]=atomic_chunk(dag, "0_1_1_3")
chunks["0_1_0_2"]=atomic_chunk(dag, "0_1_0_2")
chunks["0_1_0_3"]=atomic_chunk(dag, "0_1_0_3")
chunks["0_1_1_2"]=atomic_chunk(dag, "0_1_1_2")
chunks["0_0_0_1"]=atomic_chunk(dag, "0_0_0_1")
chunks["0_0_0_0"]=atomic_chunk(dag, "0_0_0_0")
chunks["0_0_1_0"]=atomic_chunk(dag, "0_0_1_0")
chunks["0_0_1_1"]=atomic_chunk(dag, "0_0_1_1")
chunks["0_1_1_1"]=atomic_chunk(dag, "0_1_1_1")
chunks["0_1_0_0"]=atomic_chunk(dag, "0_1_0_0")
chunks["0_1_0_1"]=atomic_chunk(dag, "0_1_0_1")
chunks["0_1_1_0"]=atomic_chunk(dag, "0_1_1_0")
chunks["0_0_2_1"]=atomic_chunk(dag, "0_0_2_1")
chunks["0_0_2_0"]=atomic_chunk(dag, "0_0_2_0")
chunks["0_0_3_0"]=atomic_chunk(dag, "0_0_3_0")
chunks["0_0_3_1"]=atomic_chunk(dag, "0_0_3_1")
chunks["0_1_3_1"]=atomic_chunk(dag, "0_1_3_1")
chunks["0_1_2_0"]=atomic_chunk(dag, "0_1_2_0")
chunks["0_1_2_1"]=atomic_chunk(dag, "0_1_2_1")
chunks["0_1_3_0"]=atomic_chunk(dag, "0_1_3_0")
chunks["0_0_2_3"]=atomic_chunk(dag, "0_0_2_3")
chunks["0_0_2_2"]=atomic_chunk(dag, "0_0_2_2")
chunks["0_0_3_2"]=atomic_chunk(dag, "0_0_3_2")
chunks["0_0_3_3"]=atomic_chunk(dag, "0_0_3_3")
chunks["0_1_3_3"]=atomic_chunk(dag, "0_1_3_3")
chunks["0_1_2_2"]=atomic_chunk(dag, "0_1_2_2")
chunks["0_1_2_3"]=atomic_chunk(dag, "0_1_2_3")
chunks["0_1_3_2"]=atomic_chunk(dag, "0_1_3_2")
chunks["0_2_2_3"]=atomic_chunk(dag, "0_2_2_3")
chunks["0_2_2_2"]=atomic_chunk(dag, "0_2_2_2")
chunks["0_2_3_2"]=atomic_chunk(dag, "0_2_3_2")
chunks["0_2_3_3"]=atomic_chunk(dag, "0_2_3_3")
chunks["0_3_3_3"]=atomic_chunk(dag, "0_3_3_3")
chunks["0_3_2_2"]=atomic_chunk(dag, "0_3_2_2")
chunks["0_3_2_3"]=atomic_chunk(dag, "0_3_2_3")
chunks["0_3_3_2"]=atomic_chunk(dag, "0_3_3_2")
chunks["0_2_0_1"]=atomic_chunk(dag, "0_2_0_1")
chunks["0_2_0_0"]=atomic_chunk(dag, "0_2_0_0")
chunks["0_2_1_0"]=atomic_chunk(dag, "0_2_1_0")
chunks["0_2_1_1"]=atomic_chunk(dag, "0_2_1_1")
chunks["0_3_1_1"]=atomic_chunk(dag, "0_3_1_1")
chunks["0_3_0_0"]=atomic_chunk(dag, "0_3_0_0")
chunks["0_3_0_1"]=atomic_chunk(dag, "0_3_0_1")
chunks["0_3_1_0"]=atomic_chunk(dag, "0_3_1_0")
chunks["0_2_0_3"]=atomic_chunk(dag, "0_2_0_3")
chunks["0_2_0_2"]=atomic_chunk(dag, "0_2_0_2")
chunks["0_2_1_2"]=atomic_chunk(dag, "0_2_1_2")
chunks["0_2_1_3"]=atomic_chunk(dag, "0_2_1_3")
chunks["0_3_1_3"]=atomic_chunk(dag, "0_3_1_3")
chunks["0_3_0_2"]=atomic_chunk(dag, "0_3_0_2")
chunks["0_3_0_3"]=atomic_chunk(dag, "0_3_0_3")
chunks["0_3_1_2"]=atomic_chunk(dag, "0_3_1_2")
chunks["0_2_2_1"]=atomic_chunk(dag, "0_2_2_1")
chunks["0_2_2_0"]=atomic_chunk(dag, "0_2_2_0")
chunks["0_2_3_0"]=atomic_chunk(dag, "0_2_3_0")
chunks["0_2_3_1"]=atomic_chunk(dag, "0_2_3_1")
chunks["0_3_3_1"]=atomic_chunk(dag, "0_3_3_1")
chunks["0_3_2_0"]=atomic_chunk(dag, "0_3_2_0")
chunks["0_3_2_1"]=atomic_chunk(dag, "0_3_2_1")
chunks["0_3_3_0"]=atomic_chunk(dag, "0_3_3_0")
chunks["1_0_0_1"].set_downstream(chunks["2_0_0_0"])
chunks["1_0_0_0"].set_downstream(chunks["2_0_0_0"])
chunks["1_0_1_0"].set_downstream(chunks["2_0_0_0"])
chunks["1_0_1_1"].set_downstream(chunks["2_0_0_0"])
chunks["1_1_1_1"].set_downstream(chunks["2_0_0_0"])
chunks["1_1_0_0"].set_downstream(chunks["2_0_0_0"])
chunks["1_1_0_1"].set_downstream(chunks["2_0_0_0"])
chunks["1_1_1_0"].set_downstream(chunks["2_0_0_0"])
chunks["0_0_0_3"].set_downstream(chunks["1_0_0_1"])
chunks["0_0_0_2"].set_downstream(chunks["1_0_0_1"])
chunks["0_0_1_2"].set_downstream(chunks["1_0_0_1"])
chunks["0_0_1_3"].set_downstream(chunks["1_0_0_1"])
chunks["0_1_1_3"].set_downstream(chunks["1_0_0_1"])
chunks["0_1_0_2"].set_downstream(chunks["1_0_0_1"])
chunks["0_1_0_3"].set_downstream(chunks["1_0_0_1"])
chunks["0_1_1_2"].set_downstream(chunks["1_0_0_1"])
chunks["0_0_0_1"].set_downstream(chunks["1_0_0_0"])
chunks["0_0_0_0"].set_downstream(chunks["1_0_0_0"])
chunks["0_0_1_0"].set_downstream(chunks["1_0_0_0"])
chunks["0_0_1_1"].set_downstream(chunks["1_0_0_0"])
chunks["0_1_1_1"].set_downstream(chunks["1_0_0_0"])
chunks["0_1_0_0"].set_downstream(chunks["1_0_0_0"])
chunks["0_1_0_1"].set_downstream(chunks["1_0_0_0"])
chunks["0_1_1_0"].set_downstream(chunks["1_0_0_0"])
chunks["0_0_2_1"].set_downstream(chunks["1_0_1_0"])
chunks["0_0_2_0"].set_downstream(chunks["1_0_1_0"])
chunks["0_0_3_0"].set_downstream(chunks["1_0_1_0"])
chunks["0_0_3_1"].set_downstream(chunks["1_0_1_0"])
chunks["0_1_3_1"].set_downstream(chunks["1_0_1_0"])
chunks["0_1_2_0"].set_downstream(chunks["1_0_1_0"])
chunks["0_1_2_1"].set_downstream(chunks["1_0_1_0"])
chunks["0_1_3_0"].set_downstream(chunks["1_0_1_0"])
chunks["0_0_2_3"].set_downstream(chunks["1_0_1_1"])
chunks["0_0_2_2"].set_downstream(chunks["1_0_1_1"])
chunks["0_0_3_2"].set_downstream(chunks["1_0_1_1"])
chunks["0_0_3_3"].set_downstream(chunks["1_0_1_1"])
chunks["0_1_3_3"].set_downstream(chunks["1_0_1_1"])
chunks["0_1_2_2"].set_downstream(chunks["1_0_1_1"])
chunks["0_1_2_3"].set_downstream(chunks["1_0_1_1"])
chunks["0_1_3_2"].set_downstream(chunks["1_0_1_1"])
chunks["0_2_2_3"].set_downstream(chunks["1_1_1_1"])
chunks["0_2_2_2"].set_downstream(chunks["1_1_1_1"])
chunks["0_2_3_2"].set_downstream(chunks["1_1_1_1"])
chunks["0_2_3_3"].set_downstream(chunks["1_1_1_1"])
chunks["0_3_3_3"].set_downstream(chunks["1_1_1_1"])
chunks["0_3_2_2"].set_downstream(chunks["1_1_1_1"])
chunks["0_3_2_3"].set_downstream(chunks["1_1_1_1"])
chunks["0_3_3_2"].set_downstream(chunks["1_1_1_1"])
chunks["0_2_0_1"].set_downstream(chunks["1_1_0_0"])
chunks["0_2_0_0"].set_downstream(chunks["1_1_0_0"])
chunks["0_2_1_0"].set_downstream(chunks["1_1_0_0"])
chunks["0_2_1_1"].set_downstream(chunks["1_1_0_0"])
chunks["0_3_1_1"].set_downstream(chunks["1_1_0_0"])
chunks["0_3_0_0"].set_downstream(chunks["1_1_0_0"])
chunks["0_3_0_1"].set_downstream(chunks["1_1_0_0"])
chunks["0_3_1_0"].set_downstream(chunks["1_1_0_0"])
chunks["0_2_0_3"].set_downstream(chunks["1_1_0_1"])
chunks["0_2_0_2"].set_downstream(chunks["1_1_0_1"])
chunks["0_2_1_2"].set_downstream(chunks["1_1_0_1"])
chunks["0_2_1_3"].set_downstream(chunks["1_1_0_1"])
chunks["0_3_1_3"].set_downstream(chunks["1_1_0_1"])
chunks["0_3_0_2"].set_downstream(chunks["1_1_0_1"])
chunks["0_3_0_3"].set_downstream(chunks["1_1_0_1"])
chunks["0_3_1_2"].set_downstream(chunks["1_1_0_1"])
chunks["0_2_2_1"].set_downstream(chunks["1_1_1_0"])
chunks["0_2_2_0"].set_downstream(chunks["1_1_1_0"])
chunks["0_2_3_0"].set_downstream(chunks["1_1_1_0"])
chunks["0_2_3_1"].set_downstream(chunks["1_1_1_0"])
chunks["0_3_3_1"].set_downstream(chunks["1_1_1_0"])
chunks["0_3_2_0"].set_downstream(chunks["1_1_1_0"])
chunks["0_3_2_1"].set_downstream(chunks["1_1_1_0"])
chunks["0_3_3_0"].set_downstream(chunks["1_1_1_0"])
