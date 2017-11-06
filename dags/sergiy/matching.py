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

params_file   = "./dags/sergiy/pinky_700_701.json"
params = json.loads(params_file)

z_start = params["params"]["mesh"]["z_start"]
z_end   = params["params"]["mesh"]["z_end"]
z_range = range(z_start, z_end)

# [1, 2, 3] -> [(1,2), (2,3)]
section_pairs = zip(z_range, z_range[1:])

nodes = []
for zs in section_pairs:
    subtask_name = str(zs)
    subtask_params = copy.deepcopy(params)
    subtask_params["params"]["mesh"]["z_start"] = zs[0]
    subtask_params["params"]["mesh"]["z_end"]   = zs[1]
    subtask_params_str = json.dumps(subtask_params).replace("'", "\\'").replace('"', '\\"')

    nodes.append(DockerWithVariablesOperator(
            ["google-secret.json"],
            mount_point="/root/.cloudvolume/secrets/",
            task_id=subtask_name,
            command='/bin/bash -c "echo \'{}\' > /clients/params.json;'.format(subtask_params_str),
                    #julia /clients/match_client.jl /clients/params.json;"'.format(),
            default_args=default_args,
            image="sergiypopo/alembic",
            dag=matching_dag
        )
    )
