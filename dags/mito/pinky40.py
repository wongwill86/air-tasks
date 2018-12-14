from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator


DAG_ID = 'mito_pinky40'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 30),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=30),
    'retry_exponential_backoff': True,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

# =============
# run-specific args
img_cvname = "gs://neuroglancer/pinky40_v11/image"
seg_cvname = "gs://neuroglancer/pinky40_v11/watershed_mst_trimmed_sem_remap"
out_cvname = "gs://neuroglancer/pinky40_v12/mitochondria/mito0_220k"
cc_cvname = "gs://neuroglancer/pinky40_v12/mitochondria/mito0_220k_objects"

# FULL VOLUME COORDS
start_coord = (10240, 7680, 0)
vol_shape   = (55296, 36864, 1024)
chunk_shape = (2048, 2048, 1024)
max_face_shape = (1024, 1024)


cc_thresh = 0.92
sz_thresh = 300
cc_dil_param = 0

coord_mip = 1
cv_mip = 0
seg_mip = 1

proc_dir_path = "gs://seunglab/nick/pinky40_mito/"
# =============

import itertools
import operator


def chunk_bboxes(vol_size, chunk_size, offset=None, mip=0):

    if mip > 0:
        mip_factor = 2 ** mip
        vol_size = downsample_coord(vol_size, mip_factor)
        chunk_size = downsample_coord(chunk_size, mip_factor)
        if offset is not None:
            offset = downsample_coord(offset, mip_factor)

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


def downsample_coord(coord, factor):
    return (coord[0]//factor, coord[1]//factor, coord[2])


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

    chunk_begin_str = " ".join(map(str, chunk_begin))
    chunk_end_str = " ".join(map(str, chunk_end))
    task_tag = "_".join(map(str, chunk_begin))

    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_ccs_" + task_tag,
        command=(f"chunk_ccs {out_cvname} {cc_cvname} {proc_dir_path}"
                 f" {cc_thresh} {sz_thresh}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
		 f" --mip {cv_mip}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag)


def merge_ccs(dag):
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_ccs",
        command=(f"merge_ccs {proc_dir_path} {sz_thresh}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag)


def remap_ids(dag, chunk_begin, chunk_end):

    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    task_tag = "_".join(map(str, chunk_begin))

    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="remap_ids" + task_tag,
        command=(f"remap_ids {cc_cvname} {cc_cvname} {proc_dir_path}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
		 f" --mip {cv_mip}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag)


def chunk_overlaps(dag, chunk_begin, chunk_end):

    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    task_tag = "_".join(map(str, chunk_begin))

    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        task_id="chunk_overlaps"+ task_tag,
        mount_point="/root/.cloudvolume/secrets",
        command=(f"chunk_overlaps {cc_cvname} {seg_cvname} {proc_dir_path}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --mip {cv_mip} --seg_mip {seg_mip}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag)


def merge_overlaps(dag):
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        task_id="merge_overlaps",
        mount_point="/root/.cloudvolume/secrets",
        command=f"merge_overlaps {proc_dir_path}",
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag)


# STEP 1: chunk_ccs
bboxes = chunk_bboxes(vol_shape, chunk_shape, 
                      offset=start_coord, mip=coord_mip)
step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]

# STEP 2: merge_ccs
step2 = merge_ccs(dag)
for chunk in step1:
    chunk.set_downstream(step2)

# STEP 3: remap_ids
step3 = [remap_ids(dag, bb[0], bb[1]) for bb in bboxes]
for chunk in step3[:-1]:
    step2.set_downstream(chunk)
    chunk.set_downstream(step3[-1])

# STEP 4: chunk_overlaps
step4 = [chunk_overlaps(dag, bb[0], bb[1]) for bb in bboxes]
for chunk in step4:
    step3[-1].set_downstream(chunk)

# STEP 5: merge_overlaps
step5 = merge_overlaps(dag)
for chunk in step4:
    chunk.set_downstream(step5)

