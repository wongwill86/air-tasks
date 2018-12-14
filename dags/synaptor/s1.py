from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator

DAG_ID = 'synaptor_s1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,11,14),
    'cactchup_by_default': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=20),
    'retry_exponential_backoff': False,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

# =============
# run-specific args
img_cvname = "gs://neuroglancer/s1_v0.1/image"
seg_cvname = "gs://neuroglancer/s1_v0.1/segmentation_0.2"
out_cvname = "gs://neuroglancer/s1_v2/psd/cleft_355k"
cleft_cvname = "gs://neuroglancer/s1_v2/clefts/full"

# VOLUME COORDS (in mip0)
start_coord = (0, 0, 0)
vol_shape   = (12288, 11264, 2048)
chunk_shape = (1024, 1024, 1024)
max_face_shape = (1024, 1024) # in mip 1

base_mip = 1 # mip for cleft output
base_res = (12, 12, 30)
asynet_mip = 1 # num downsamplings on top of base
asynet_res = (24, 24, 30)

cc_thresh = 0.4
sz_thresh1 = 25
sz_thresh2 = 100
cc_dil_param = 0

num_samples = 2
asyn_dil_param = 5
patch_sz = (40, 40, 18)

voxel_res = (12, 12, 30)
dist_thr = 1000

proc_dir_path = "gs://seunglab/nick/181114_s1"
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


def tup2str(t): return " ".join(map(str, t))


def chunk_ccs(dag, chunk_begin, chunk_end):

    chunk_begin_str = tup2str(chunk_begin)
    chunk_end_str = tup2str(chunk_end)
    res_str = tup2str(base_res)

    return DockerWithVariablesOperator(
        ["google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_ccs_" + "_".join(map(str, chunk_begin)),
        command=(f"chunk_ccs {out_cvname} {cleft_cvname} {proc_dir_path}"
                 f" {cc_thresh} {sz_thresh1}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --mip {res_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def merge_ccs(dag):

    faceshape_str = tup2str(max_face_shape)

    return DockerWithVariablesOperator(
        ["google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_ccs",
        command=(f"merge_ccs {proc_dir_path} {sz_thresh1}"
                 f" --max_face_shape {faceshape_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def asynet_pass(dag, chunk_begin, chunk_end, base_begin, base_end):

    chunk_begin_str = tup2str(chunk_begin)
    chunk_end_str = tup2str(chunk_end)
    base_begin_str = tup2str(base_begin)
    base_end_str = tup2str(base_end)
    patchsz_str = tup2str(patch_sz)
    res_str = tup2str(asynet_res)

    return DockerWithVariablesOperator(
        ["google-secret.json"],
        host_args={"runtime": "nvidia"},
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_edges" + "_".join(map(str,chunk_begin)),
        command=(f"chunk_edges {img_cvname} {cleft_cvname} {seg_cvname}"
                 f" {proc_dir_path} {num_samples} {asyn_dil_param}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --patchsz {patchsz_str} --resolution {res_str}"
                 f" --num_downsamples {asynet_mip}"
                 f" --base_res_begin {base_begin_str}"
                 f" --base_res_end {base_end_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="gpu",
        dag=dag
        )


def merge_edges(dag):

    res_str = tup2str(voxel_res)

    return DockerWithVariablesOperator(
        ["google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_edges",
        command=(f"merge_edges {proc_dir_path} {dist_thr} {sz_thresh2}"
                 f" --voxel_res {res_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def remap_ids(dag, chunk_begin, chunk_end):

    chunk_begin_str = tup2str(chunk_begin)
    chunk_end_str = tup2str(chunk_end)
    res_str = tup2str(base_res)

    return DockerWithVariablesOperator(
        ["google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="remap_ids" + "_".join(map(str,chunk_begin)),
        command=(f"remap_ids {cleft_cvname} {cleft_cvname} {proc_dir_path}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --mip {res_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


bboxes = chunk_bboxes(vol_shape, chunk_shape, 
                      offset=start_coord, mip=base_mip)
asynet_bboxes = chunk_bboxes(vol_shape, chunk_shape,
                             offset=start_coord, mip=asynet_mip+base_mip)

# STEP 1: chunk_ccs
step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]

# STEP 2: merge_ccs
step2 = merge_ccs(dag)
for chunk in step1:
    chunk.set_downstream(step2)

# STEP 3: asynet pass
step3 = [asynet_pass(dag, abb[0], abb[1], bb[0], bb[1]) 
         for (abb,bb) in zip(asynet_bboxes, bboxes)]
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
