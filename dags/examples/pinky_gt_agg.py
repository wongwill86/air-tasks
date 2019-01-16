from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.utils.weight_rule import WeightRule

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 6, 20),
    'cactchup_by_default': False,
    'retries': 100,
    'retry_delay': timedelta(seconds=3),
    'retry_exponential_backoff': True,
    }
dag = DAG(
    "pinky_gt_agg", default_args=default_args, schedule_interval=None)

ws_image = "ranlu/watershed:pinkygt"
agg_image = "ranlu/agglomeration:pinkygt"
cmd_proto = '/bin/bash -c "mkdir $AIRFLOW_TMP_DIR/work && cd $AIRFLOW_TMP_DIR/work && {} && rm -rf $AIRFLOW_TMP_DIR/work || {{ rm -rf $AIRFLOW_TMP_DIR/work; exit 111; }}"'
config_mounts = ['neuroglancer-google-secret.json','google-secret.json', 'config_pinky_gt_agg.sh']

def atomic_chunks_op(dag, tags):
    cmdlist = " && ".join(["/root/agg/scripts/atomic_chunk_me.sh /root/agg/test/{}.json".format(tag) for tag in tags])
    return DockerWithVariablesOperator(
        config_mounts,
        host_args={'shm_size': '8G'},
        mount_point='/root/.cloudvolume/secrets/',
        task_id='atomic_chunk_' + tags[0],
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=agg_image,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=120),
        queue="atomic",
        dag=dag
    )

def composite_chunks_op(dag, tags):
    cmdlist = " && ".join(["/root/agg/scripts/composite_chunk_me.sh /root/agg/test/{}.json".format(tag) for tag in tags])
    return DockerWithVariablesOperator(
        config_mounts,
        host_args={'shm_size': '8G'},
        mount_point='/root/.cloudvolume/secrets/',
        task_id='composite_chunk_' + tags[0],
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=agg_image,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=60),
        queue="atomic",
        dag=dag
    )

def composite_chunks_long_op(dag, tags):
    cmdlist = " && ".join(["/root/agg/scripts/composite_chunk_me.sh /root/agg/test/{}.json".format(tag) for tag in tags])
    return DockerWithVariablesOperator(
        config_mounts,
        host_args={'shm_size': '8G'},
        mount_point='/root/.cloudvolume/secrets/',
        task_id='composite_chunk_' + tags[0],
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=agg_image,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(hours=24),
        queue="composite",
        dag=dag
    )

def remap_chunks_op(dag, tags):
    cmdlist = " && ".join(["/root/ws/scripts/remap_chunk_agg.sh /root/ws/test/{}.json".format(tag) for tag in tags])
    return DockerWithVariablesOperator(
        config_mounts,
        host_args={'shm_size': '8G'},
        mount_point='/root/.cloudvolume/secrets/',
        task_id='remap_chunk_' + tags[0],
        command=cmd_proto.format(cmdlist),
        default_args=default_args,
        image=ws_image,
        weight_rule=WeightRule.ABSOLUTE,
        execution_timeout=timedelta(minutes=30),
        queue="atomic",
        dag=dag
    )

generate_chunks = {}
remap_chunks = {}
generate_chunks["2_0_0_0"]=composite_chunks_op(dag, ["2_0_0_0"])
generate_chunks["1_0_0_0"]=composite_chunks_op(dag, ["1_0_0_0"])
generate_chunks["1_0_0_0_0"]=atomic_chunks_op(dag, ["0_0_0_0"])
remap_chunks["1_0_0_0_0"]=remap_chunks_op(dag, ["0_0_0_0"])
generate_chunks["1_0_0_0_1"]=atomic_chunks_op(dag, ["0_0_0_1"])
remap_chunks["1_0_0_0_1"]=remap_chunks_op(dag, ["0_0_0_1"])
generate_chunks["1_0_0_0_2"]=atomic_chunks_op(dag, ["0_0_1_0"])
remap_chunks["1_0_0_0_2"]=remap_chunks_op(dag, ["0_0_1_0"])
generate_chunks["1_0_0_0_3"]=atomic_chunks_op(dag, ["0_0_1_1"])
remap_chunks["1_0_0_0_3"]=remap_chunks_op(dag, ["0_0_1_1"])
generate_chunks["1_0_0_0_4"]=atomic_chunks_op(dag, ["0_1_0_0"])
remap_chunks["1_0_0_0_4"]=remap_chunks_op(dag, ["0_1_0_0"])
generate_chunks["1_0_0_0_5"]=atomic_chunks_op(dag, ["0_1_0_1"])
remap_chunks["1_0_0_0_5"]=remap_chunks_op(dag, ["0_1_0_1"])
generate_chunks["1_0_0_0_6"]=atomic_chunks_op(dag, ["0_1_1_0"])
remap_chunks["1_0_0_0_6"]=remap_chunks_op(dag, ["0_1_1_0"])
generate_chunks["1_0_0_0_7"]=atomic_chunks_op(dag, ["0_1_1_1"])
remap_chunks["1_0_0_0_7"]=remap_chunks_op(dag, ["0_1_1_1"])
generate_chunks["1_0_0_1"]=composite_chunks_op(dag, ["1_0_0_1"])
generate_chunks["1_0_0_1_0"]=atomic_chunks_op(dag, ["0_0_0_2"])
remap_chunks["1_0_0_1_0"]=remap_chunks_op(dag, ["0_0_0_2"])
generate_chunks["1_0_0_1_1"]=atomic_chunks_op(dag, ["0_0_0_3"])
remap_chunks["1_0_0_1_1"]=remap_chunks_op(dag, ["0_0_0_3"])
generate_chunks["1_0_0_1_2"]=atomic_chunks_op(dag, ["0_0_1_2"])
remap_chunks["1_0_0_1_2"]=remap_chunks_op(dag, ["0_0_1_2"])
generate_chunks["1_0_0_1_3"]=atomic_chunks_op(dag, ["0_0_1_3"])
remap_chunks["1_0_0_1_3"]=remap_chunks_op(dag, ["0_0_1_3"])
generate_chunks["1_0_0_1_4"]=atomic_chunks_op(dag, ["0_1_0_2"])
remap_chunks["1_0_0_1_4"]=remap_chunks_op(dag, ["0_1_0_2"])
generate_chunks["1_0_0_1_5"]=atomic_chunks_op(dag, ["0_1_0_3"])
remap_chunks["1_0_0_1_5"]=remap_chunks_op(dag, ["0_1_0_3"])
generate_chunks["1_0_0_1_6"]=atomic_chunks_op(dag, ["0_1_1_2"])
remap_chunks["1_0_0_1_6"]=remap_chunks_op(dag, ["0_1_1_2"])
generate_chunks["1_0_0_1_7"]=atomic_chunks_op(dag, ["0_1_1_3"])
remap_chunks["1_0_0_1_7"]=remap_chunks_op(dag, ["0_1_1_3"])
generate_chunks["1_0_1_0"]=composite_chunks_op(dag, ["1_0_1_0"])
generate_chunks["1_0_1_0_0"]=atomic_chunks_op(dag, ["0_0_2_0"])
remap_chunks["1_0_1_0_0"]=remap_chunks_op(dag, ["0_0_2_0"])
generate_chunks["1_0_1_0_1"]=atomic_chunks_op(dag, ["0_0_2_1"])
remap_chunks["1_0_1_0_1"]=remap_chunks_op(dag, ["0_0_2_1"])
generate_chunks["1_0_1_0_2"]=atomic_chunks_op(dag, ["0_0_3_0"])
remap_chunks["1_0_1_0_2"]=remap_chunks_op(dag, ["0_0_3_0"])
generate_chunks["1_0_1_0_3"]=atomic_chunks_op(dag, ["0_0_3_1"])
remap_chunks["1_0_1_0_3"]=remap_chunks_op(dag, ["0_0_3_1"])
generate_chunks["1_0_1_0_4"]=atomic_chunks_op(dag, ["0_1_2_0"])
remap_chunks["1_0_1_0_4"]=remap_chunks_op(dag, ["0_1_2_0"])
generate_chunks["1_0_1_0_5"]=atomic_chunks_op(dag, ["0_1_2_1"])
remap_chunks["1_0_1_0_5"]=remap_chunks_op(dag, ["0_1_2_1"])
generate_chunks["1_0_1_0_6"]=atomic_chunks_op(dag, ["0_1_3_0"])
remap_chunks["1_0_1_0_6"]=remap_chunks_op(dag, ["0_1_3_0"])
generate_chunks["1_0_1_0_7"]=atomic_chunks_op(dag, ["0_1_3_1"])
remap_chunks["1_0_1_0_7"]=remap_chunks_op(dag, ["0_1_3_1"])
generate_chunks["1_0_1_1"]=composite_chunks_op(dag, ["1_0_1_1"])
generate_chunks["1_0_1_1_0"]=atomic_chunks_op(dag, ["0_0_2_2"])
remap_chunks["1_0_1_1_0"]=remap_chunks_op(dag, ["0_0_2_2"])
generate_chunks["1_0_1_1_1"]=atomic_chunks_op(dag, ["0_0_2_3"])
remap_chunks["1_0_1_1_1"]=remap_chunks_op(dag, ["0_0_2_3"])
generate_chunks["1_0_1_1_2"]=atomic_chunks_op(dag, ["0_0_3_2"])
remap_chunks["1_0_1_1_2"]=remap_chunks_op(dag, ["0_0_3_2"])
generate_chunks["1_0_1_1_3"]=atomic_chunks_op(dag, ["0_0_3_3"])
remap_chunks["1_0_1_1_3"]=remap_chunks_op(dag, ["0_0_3_3"])
generate_chunks["1_0_1_1_4"]=atomic_chunks_op(dag, ["0_1_2_2"])
remap_chunks["1_0_1_1_4"]=remap_chunks_op(dag, ["0_1_2_2"])
generate_chunks["1_0_1_1_5"]=atomic_chunks_op(dag, ["0_1_2_3"])
remap_chunks["1_0_1_1_5"]=remap_chunks_op(dag, ["0_1_2_3"])
generate_chunks["1_0_1_1_6"]=atomic_chunks_op(dag, ["0_1_3_2"])
remap_chunks["1_0_1_1_6"]=remap_chunks_op(dag, ["0_1_3_2"])
generate_chunks["1_0_1_1_7"]=atomic_chunks_op(dag, ["0_1_3_3"])
remap_chunks["1_0_1_1_7"]=remap_chunks_op(dag, ["0_1_3_3"])
generate_chunks["1_1_0_0"]=composite_chunks_op(dag, ["1_1_0_0"])
generate_chunks["1_1_0_0_0"]=atomic_chunks_op(dag, ["0_2_0_0"])
remap_chunks["1_1_0_0_0"]=remap_chunks_op(dag, ["0_2_0_0"])
generate_chunks["1_1_0_0_1"]=atomic_chunks_op(dag, ["0_2_0_1"])
remap_chunks["1_1_0_0_1"]=remap_chunks_op(dag, ["0_2_0_1"])
generate_chunks["1_1_0_0_2"]=atomic_chunks_op(dag, ["0_2_1_0"])
remap_chunks["1_1_0_0_2"]=remap_chunks_op(dag, ["0_2_1_0"])
generate_chunks["1_1_0_0_3"]=atomic_chunks_op(dag, ["0_2_1_1"])
remap_chunks["1_1_0_0_3"]=remap_chunks_op(dag, ["0_2_1_1"])
generate_chunks["1_1_0_0_4"]=atomic_chunks_op(dag, ["0_3_0_0"])
remap_chunks["1_1_0_0_4"]=remap_chunks_op(dag, ["0_3_0_0"])
generate_chunks["1_1_0_0_5"]=atomic_chunks_op(dag, ["0_3_0_1"])
remap_chunks["1_1_0_0_5"]=remap_chunks_op(dag, ["0_3_0_1"])
generate_chunks["1_1_0_0_6"]=atomic_chunks_op(dag, ["0_3_1_0"])
remap_chunks["1_1_0_0_6"]=remap_chunks_op(dag, ["0_3_1_0"])
generate_chunks["1_1_0_0_7"]=atomic_chunks_op(dag, ["0_3_1_1"])
remap_chunks["1_1_0_0_7"]=remap_chunks_op(dag, ["0_3_1_1"])
generate_chunks["1_1_0_1"]=composite_chunks_op(dag, ["1_1_0_1"])
generate_chunks["1_1_0_1_0"]=atomic_chunks_op(dag, ["0_2_0_2"])
remap_chunks["1_1_0_1_0"]=remap_chunks_op(dag, ["0_2_0_2"])
generate_chunks["1_1_0_1_1"]=atomic_chunks_op(dag, ["0_2_0_3"])
remap_chunks["1_1_0_1_1"]=remap_chunks_op(dag, ["0_2_0_3"])
generate_chunks["1_1_0_1_2"]=atomic_chunks_op(dag, ["0_2_1_2"])
remap_chunks["1_1_0_1_2"]=remap_chunks_op(dag, ["0_2_1_2"])
generate_chunks["1_1_0_1_3"]=atomic_chunks_op(dag, ["0_2_1_3"])
remap_chunks["1_1_0_1_3"]=remap_chunks_op(dag, ["0_2_1_3"])
generate_chunks["1_1_0_1_4"]=atomic_chunks_op(dag, ["0_3_0_2"])
remap_chunks["1_1_0_1_4"]=remap_chunks_op(dag, ["0_3_0_2"])
generate_chunks["1_1_0_1_5"]=atomic_chunks_op(dag, ["0_3_0_3"])
remap_chunks["1_1_0_1_5"]=remap_chunks_op(dag, ["0_3_0_3"])
generate_chunks["1_1_0_1_6"]=atomic_chunks_op(dag, ["0_3_1_2"])
remap_chunks["1_1_0_1_6"]=remap_chunks_op(dag, ["0_3_1_2"])
generate_chunks["1_1_0_1_7"]=atomic_chunks_op(dag, ["0_3_1_3"])
remap_chunks["1_1_0_1_7"]=remap_chunks_op(dag, ["0_3_1_3"])
generate_chunks["1_1_1_0"]=composite_chunks_op(dag, ["1_1_1_0"])
generate_chunks["1_1_1_0_0"]=atomic_chunks_op(dag, ["0_2_2_0"])
remap_chunks["1_1_1_0_0"]=remap_chunks_op(dag, ["0_2_2_0"])
generate_chunks["1_1_1_0_1"]=atomic_chunks_op(dag, ["0_2_2_1"])
remap_chunks["1_1_1_0_1"]=remap_chunks_op(dag, ["0_2_2_1"])
generate_chunks["1_1_1_0_2"]=atomic_chunks_op(dag, ["0_2_3_0"])
remap_chunks["1_1_1_0_2"]=remap_chunks_op(dag, ["0_2_3_0"])
generate_chunks["1_1_1_0_3"]=atomic_chunks_op(dag, ["0_2_3_1"])
remap_chunks["1_1_1_0_3"]=remap_chunks_op(dag, ["0_2_3_1"])
generate_chunks["1_1_1_0_4"]=atomic_chunks_op(dag, ["0_3_2_0"])
remap_chunks["1_1_1_0_4"]=remap_chunks_op(dag, ["0_3_2_0"])
generate_chunks["1_1_1_0_5"]=atomic_chunks_op(dag, ["0_3_2_1"])
remap_chunks["1_1_1_0_5"]=remap_chunks_op(dag, ["0_3_2_1"])
generate_chunks["1_1_1_0_6"]=atomic_chunks_op(dag, ["0_3_3_0"])
remap_chunks["1_1_1_0_6"]=remap_chunks_op(dag, ["0_3_3_0"])
generate_chunks["1_1_1_0_7"]=atomic_chunks_op(dag, ["0_3_3_1"])
remap_chunks["1_1_1_0_7"]=remap_chunks_op(dag, ["0_3_3_1"])
generate_chunks["1_1_1_1"]=composite_chunks_op(dag, ["1_1_1_1"])
generate_chunks["1_1_1_1_0"]=atomic_chunks_op(dag, ["0_2_2_2"])
remap_chunks["1_1_1_1_0"]=remap_chunks_op(dag, ["0_2_2_2"])
generate_chunks["1_1_1_1_1"]=atomic_chunks_op(dag, ["0_2_2_3"])
remap_chunks["1_1_1_1_1"]=remap_chunks_op(dag, ["0_2_2_3"])
generate_chunks["1_1_1_1_2"]=atomic_chunks_op(dag, ["0_2_3_2"])
remap_chunks["1_1_1_1_2"]=remap_chunks_op(dag, ["0_2_3_2"])
generate_chunks["1_1_1_1_3"]=atomic_chunks_op(dag, ["0_2_3_3"])
remap_chunks["1_1_1_1_3"]=remap_chunks_op(dag, ["0_2_3_3"])
generate_chunks["1_1_1_1_4"]=atomic_chunks_op(dag, ["0_3_2_2"])
remap_chunks["1_1_1_1_4"]=remap_chunks_op(dag, ["0_3_2_2"])
generate_chunks["1_1_1_1_5"]=atomic_chunks_op(dag, ["0_3_2_3"])
remap_chunks["1_1_1_1_5"]=remap_chunks_op(dag, ["0_3_2_3"])
generate_chunks["1_1_1_1_6"]=atomic_chunks_op(dag, ["0_3_3_2"])
remap_chunks["1_1_1_1_6"]=remap_chunks_op(dag, ["0_3_3_2"])
generate_chunks["1_1_1_1_7"]=atomic_chunks_op(dag, ["0_3_3_3"])
remap_chunks["1_1_1_1_7"]=remap_chunks_op(dag, ["0_3_3_3"])
generate_chunks["1_0_0_0"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_0_0_1"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_0_1_0"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_0_1_1"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_1_0_0"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_1_0_1"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_1_1_0"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_1_1_1"].set_downstream(generate_chunks["2_0_0_0"])
generate_chunks["1_0_0_0_0"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_0"])
generate_chunks["1_0_0_0_1"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_1"])
generate_chunks["1_0_0_0_2"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_2"])
generate_chunks["1_0_0_0_3"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_3"])
generate_chunks["1_0_0_0_4"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_4"])
generate_chunks["1_0_0_0_5"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_5"])
generate_chunks["1_0_0_0_6"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_6"])
generate_chunks["1_0_0_0_7"].set_downstream(generate_chunks["1_0_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_0_7"])
generate_chunks["1_0_0_1_0"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_0"])
generate_chunks["1_0_0_1_1"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_1"])
generate_chunks["1_0_0_1_2"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_2"])
generate_chunks["1_0_0_1_3"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_3"])
generate_chunks["1_0_0_1_4"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_4"])
generate_chunks["1_0_0_1_5"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_5"])
generate_chunks["1_0_0_1_6"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_6"])
generate_chunks["1_0_0_1_7"].set_downstream(generate_chunks["1_0_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_0_1_7"])
generate_chunks["1_0_1_0_0"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_0"])
generate_chunks["1_0_1_0_1"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_1"])
generate_chunks["1_0_1_0_2"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_2"])
generate_chunks["1_0_1_0_3"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_3"])
generate_chunks["1_0_1_0_4"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_4"])
generate_chunks["1_0_1_0_5"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_5"])
generate_chunks["1_0_1_0_6"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_6"])
generate_chunks["1_0_1_0_7"].set_downstream(generate_chunks["1_0_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_0_7"])
generate_chunks["1_0_1_1_0"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_0"])
generate_chunks["1_0_1_1_1"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_1"])
generate_chunks["1_0_1_1_2"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_2"])
generate_chunks["1_0_1_1_3"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_3"])
generate_chunks["1_0_1_1_4"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_4"])
generate_chunks["1_0_1_1_5"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_5"])
generate_chunks["1_0_1_1_6"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_6"])
generate_chunks["1_0_1_1_7"].set_downstream(generate_chunks["1_0_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_0_1_1_7"])
generate_chunks["1_1_0_0_0"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_0"])
generate_chunks["1_1_0_0_1"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_1"])
generate_chunks["1_1_0_0_2"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_2"])
generate_chunks["1_1_0_0_3"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_3"])
generate_chunks["1_1_0_0_4"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_4"])
generate_chunks["1_1_0_0_5"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_5"])
generate_chunks["1_1_0_0_6"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_6"])
generate_chunks["1_1_0_0_7"].set_downstream(generate_chunks["1_1_0_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_0_7"])
generate_chunks["1_1_0_1_0"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_0"])
generate_chunks["1_1_0_1_1"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_1"])
generate_chunks["1_1_0_1_2"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_2"])
generate_chunks["1_1_0_1_3"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_3"])
generate_chunks["1_1_0_1_4"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_4"])
generate_chunks["1_1_0_1_5"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_5"])
generate_chunks["1_1_0_1_6"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_6"])
generate_chunks["1_1_0_1_7"].set_downstream(generate_chunks["1_1_0_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_0_1_7"])
generate_chunks["1_1_1_0_0"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_0"])
generate_chunks["1_1_1_0_1"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_1"])
generate_chunks["1_1_1_0_2"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_2"])
generate_chunks["1_1_1_0_3"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_3"])
generate_chunks["1_1_1_0_4"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_4"])
generate_chunks["1_1_1_0_5"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_5"])
generate_chunks["1_1_1_0_6"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_6"])
generate_chunks["1_1_1_0_7"].set_downstream(generate_chunks["1_1_1_0"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_0_7"])
generate_chunks["1_1_1_1_0"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_0"])
generate_chunks["1_1_1_1_1"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_1"])
generate_chunks["1_1_1_1_2"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_2"])
generate_chunks["1_1_1_1_3"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_3"])
generate_chunks["1_1_1_1_4"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_4"])
generate_chunks["1_1_1_1_5"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_5"])
generate_chunks["1_1_1_1_6"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_6"])
generate_chunks["1_1_1_1_7"].set_downstream(generate_chunks["1_1_1_1"])
generate_chunks["2_0_0_0"].set_downstream(remap_chunks["1_1_1_1_7"])
