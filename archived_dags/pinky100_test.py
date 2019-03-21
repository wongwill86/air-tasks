from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator

DAG_ID = 'pinky100_test'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,8,19),
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
img_cvname = "gs://neuroglancer/pinky100_v0/son_of_alignment_v15_rechunked"
seg_cvname = "gs://neuroglancer/pinky100_v0/seg/lost_no-random/bbox1_0"
out_cvname = "gs://neuroglancer/pinky100_v0/psd/mip1_d2_1175k"
cleft_cvname = "gs://neuroglancer/pinky100_v0/clefts/tests"
cleft_out_cvname = "gs://neuroglancer/pinky100_v0/clefts/tests2"
wshed_cvname = "gs://neuroglancer/pinky100_v0/ws/lost_no-random/bbox1_0"

# VOLUME COORDS (in mip0)
#start_coord = (36192,30558,21)
#vol_shape   = (86016,51200,2136)
start_coord = (79200,56158,21)
vol_shape   = (6144,6144,2136)
chunk_shape = (2048,2048,1068)
max_face_shape = (1068,1068)

mip=1
asynet_mip=1
img_mip=2
seg_mip=1

cc_thresh = 0.25
sz_thresh1 = 25
sz_thresh2 = 50
cc_dil_param = 0

num_samples = 2
asyn_dil_param = 5
patch_sz = (40, 40, 18)

voxel_res = (8, 8, 40)
dist_thr = 1000

proc_dir_path = "gs://seunglab/nick/pinky100_test2"
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


def chunk_ccs(dag, chunk_begin, chunk_end):

    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))

    return DockerWithVariablesOperator(
        ["google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_ccs_" + "_".join(map(str,chunk_begin)),
        command=("chunk_ccs {out_cvname} {cleft_cvname} {proc_dir_path} " +
                 "{cc_thresh} {sz_thresh} " +
                 "--chunk_begin {chunk_begin_str} " +
                 "--chunk_end {chunk_end_str}"
                 ).format(out_cvname=out_cvname, cleft_cvname=cleft_cvname,
                          proc_dir_path=proc_dir_path, cc_thresh=cc_thresh,
                          sz_thresh=sz_thresh1, cc_dil_param=cc_dil_param,
                          chunk_begin_str=chunk_begin_str,
                          chunk_end_str=chunk_end_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def merge_ccs(dag):
    faceshape_str = " ".join(map(str,max_face_shape))
    return DockerWithVariablesOperator(
        ["google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_ccs",
        command=("merge_ccs {proc_dir_path} {sz_thresh} --max_face_shape {faceshape}"
                      ).format(proc_dir_path=proc_dir_path, sz_thresh=sz_thresh1,
                               faceshape=faceshape_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def asynet_pass(dag, chunk_begin, chunk_end, mip0_begin, mip0_end):

    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    mip0_begin_str = " ".join(map(str,mip0_begin))
    mip0_end_str = " ".join(map(str,mip0_end))
    patchsz_str = " ".join(map(str,patch_sz))

    return DockerWithVariablesOperator(
        ["google-secret.json","aws-secret.json"],
        host_args={"runtime": "nvidia"},
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_edges" + "_".join(map(str,chunk_begin)),
        command=(f"chunk_edges {img_cvname} {cleft_cvname} {seg_cvname} " +
                      f"{proc_dir_path} {num_samples} {asyn_dil_param} " +
                      f"--chunk_begin {chunk_begin_str} " +
                      f"--chunk_end {chunk_end_str} " +
                      f"--wshed_cvname {wshed_cvname} " +
                      f"--patchsz {patchsz_str} " +
                      f"--img_mip {img_mip} " +
                      f"--seg_mip {seg_mip} " +
                      f"--mip {asynet_mip} " +
                      f"--mip0_begin {mip0_begin_str} --mip0_end {mip0_end_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="gpu",
        dag=dag
        )


def merge_edges(dag):
    voxel_res_str = " ".join(map(str,voxel_res))
    return DockerWithVariablesOperator(
        ["google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_edges",
        command=("merge_edges {proc_dir_path} {dist_thr} {size_thr} " +
                      "--voxel_res {voxel_res_str} "
                      ).format(proc_dir_path=proc_dir_path, dist_thr=dist_thr,
                               size_thr=sz_thresh2, voxel_res_str=voxel_res_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def remap_ids(dag, chunk_begin, chunk_end):
    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    return DockerWithVariablesOperator(
        ["google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="remap_ids" + "_".join(map(str,chunk_begin)),
        command=("remap_ids {cleft_cvname} {cleft_out_cvname} {proc_dir_path} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str}"
                      ).format(cleft_cvname=cleft_cvname,
                               cleft_out_cvname=cleft_out_cvname,
                               proc_dir_path=proc_dir_path,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord, mip=mip)
asynet_bboxes = chunk_bboxes(vol_shape, chunk_shape,
                             offset=start_coord, 
                             mip=asynet_mip+mip)

# STEP 1: chunk_ccs
step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]

# STEP 2: merge_ccs
step2 = merge_ccs(dag)
for chunk in step1:
    chunk.set_downstream(step2)

# STEP 3: asynet pass
step3 = [asynet_pass(dag, bb[0], bb[1], mip1[0], mip1[1]) 
         for (bb,mip1) in zip(asynet_bboxes,bboxes)]
for chunk in step3:
    step2.set_downstream(chunk)

# STEP 4: merge edges
step4 = merge_edges(dag)
for chunk in step3:
    chunk.set_downstream(step4)

# STEP 5: remap_ids
step5 = [remap_ids(dag, bb[0], bb[1]) for bb in bboxes]
for chunk in step5:
    step4.set_downstream(chunk)