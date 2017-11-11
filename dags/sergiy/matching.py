from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator
import json
import copy

DAG_ID = 'matching'

default_args = {
    'owner': 'sergiy',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

# ####################### SCHEDULER #################################
matching_dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

params_file   = "./dags/sergiy/drosophila.json"

with open(params_file) as f:
    params = json.load(f)

z_start = params["params"]["mesh"]["z_start"]
z_stop  = params["params"]["mesh"]["z_stop"]
z_range = range(z_start, z_stop + 1)

# [1, 2, 3] -> [(1,2), (2,3)]
section_pairs = zip(z_range, z_range[1:])

nodes = []
for zs in section_pairs:
    subtask_name = "{}-{}".format(zs[0], zs[1])
    subtask_params = copy.deepcopy(params)

    subtask_params["params"]["mesh"]["z_start"] = zs[0]
    subtask_params["params"]["mesh"]["z_stop"]  = zs[1]

    subtask_params_str = json.dumps(subtask_params).replace("'", "\\'").replace('"', '\\"')
    nodes.append(DockerWithVariablesOperator(
            ["google-secret.json"],
            mount_point="/root/.cloudvolume/secrets/",
            task_id=subtask_name,
            command='/bin/bash -c "echo \'{}\' > /tasks/params.json; \
                    julia /tasks/match_task.jl /tasks/params.json 4;"'.format(subtask_params_str),
                    #cat /clients/params.json;"'.format(subtask_params_str),
            default_args=default_args,
            image="sergiypopo/alembic:generic",
            dag=matching_dag
        )
    )
