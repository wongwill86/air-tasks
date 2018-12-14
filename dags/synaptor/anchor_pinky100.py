from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator

DAG_ID = 'pinky100_reanchor'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,11,3),
    'cactchup_by_default': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=30),
    'retry_exponential_backoff': False,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

# =============
# run-specific args
seg_cvname = "gs://neuroglancer/pinky100_v0/seg/lost_no-random/bbox1_0"
cleft_out_cvname = "gs://neuroglancer/pinky100_v0/clefts/mip1_d2_1175k"
wshed_cvname = "gs://neuroglancer/pinky100_v0/ws/lost_no-random/bbox1_0"

# VOLUME COORDS (in mip0)
start_coord = (36192,30558,21)
vol_shape   = (86016,51200,2136)
chunk_shape = (2048,2048,1068)

mip=1
voxel_res = (8, 8, 40)
min_box_width = (100, 100, 5)

proc_dir_path = "gs://seunglab/nick/pinky100"
# =============

import itertools
import operator


def chunk_bboxes(vol_size, chunk_size, offset=None, mip=0):

    if mip > 0:
        mip_factor = 2 ** mip
        vol_size = (vol_size[0]//mip_factor,
                    vol_size[1]//mip_factor,
                    vol_size[2])

        chunk_size = (chunk_size[0]//mip_factor,
                      chunk_size[1]//mip_factor,
                      chunk_size[2])

        if offset is not None:
            offset = (offset[0]//mip_factor,
                      offset[1]//mip_factor,
                      offset[2])

    x_bnds = bounds1D(vol_size[0], chunk_size[0])
    y_bnds = bounds1D(vol_size[1], chunk_size[1])
    z_bnds = bounds1D(vol_size[2], chunk_size[2])

    bboxes = [tuple(zip(xs, ys, zs))
              for (xs, ys, zs) in itertools.product(x_bnds, y_bnds, z_bnds)]

    if offset is not None:
        bboxes = [(tuple(map(operator.add, bb[0], offset)),
                   tuple(map(operator.add, bb[1], offset)))
                  for bb in bboxes]

    return bboxes


def bounds1D(full_width, step_size):

    assert step_size > 0, "invalid step_size: {}".format(step_size)
    assert full_width > 0, "invalid volume_width: {}".format(full_width)

    start = 0
    end = step_size

    bounds = []
    while end < full_width:
        bounds.append((start, end))

        start += step_size
        end += step_size

    # last window
    bounds.append((start, end))

    return bounds
# =============


def chunk_anchors(dag, chunk_begin, chunk_end):

    chunk_begin_str = " ".join(map(str, chunk_begin))
    chunk_end_str = " ".join(map(str, chunk_end))
    voxel_res_str = " ".join(map(str, voxel_res))
    min_box_width_str = " ".join(map(str, min_box_width))

    return DockerWithVariablesOperator(
        ["google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_anchors_" + "_".join(map(str,chunk_begin)),
        command=(f"chunk_anchors {cleft_out_cvname} {seg_cvname} {proc_dir_path}"
                 f" --wshed_cvname {wshed_cvname}"
                 f" --min_box_width {min_box_width_str}"
                 f" --voxel_res {voxel_res_str}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )

bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord, mip=mip)

# only step
step1 = [chunk_anchors(dag, bb[0], bb[1]) for bb in bboxes]
