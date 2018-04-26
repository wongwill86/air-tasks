from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator

DAG_ID = 'mito_pinky40'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,4,24),
    'cactchup_by_default': False,
    'retries': 10,
    'retry_delay': timedelta(seconds=2),
    'max_retry_delay': timedelta(minutes=10),
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
out_cvname = "gs://neuroglancer/pinky40_v11/mitochondria2"
cc_cvname = "gs://neuroglancer/pinky40_v11/mito_objects"
#cleft_cvname = "gs://neuroglancer/pinky40_v11/clefts"
#wshed_cvname = "gs://neuroglancer/pinky40_v11/watershed"
proc_dir_path = "gs://neuroglancer/agataf/pinky40"


# FULL VOLUME COORDS
start_coord = (5120,2048,0)
vol_shape   = (28672,20480,1024)
chunk_shape = (1024,1024,1024)


# NICK USES (in MIP 0):
start_coord = (5120,3840,0)
vol_shape   = (27648,18432,1024)
chunk_shape = (1024,1024,1024)

# NICK USES (in MIP 0):
# start_coord = (10240,7680,0)
# vol_shape   = (55296,36864,1024)
# chunk_shape = (1024,1024,1024)

# TEST VOLUME COORDS
#start_coord = (45696, 9920, 320)
#vol_shape = (2048, 2048, 256)
#chunk_shape = (1024, 1024, 128)

cc_thresh = 0.19
sz_thresh = 460
#sz_thresh2 = 500 #???
cc_dil_param = 0


num_samples = 2
asyn_dil_param = 5
patch_sz = (160, 160, 16)

voxel_res = (4, 4, 40)
dist_thr = 1000
mip = 1

# =============

import itertools
import operator


def chunk_bboxes(vol_size, chunk_size, offset=None):

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
        ["project_name","google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_ccs_" + "_".join(map(str,chunk_begin)),
        command=("chunk_ccs {out_cvname} {cc_cvname} {proc_dir_path} " +
                 "{cc_thresh} {sz_thresh} " +
                 "--chunk_begin {chunk_begin_str} " +
                 "--chunk_end {chunk_end_str} " +
		 "--mip {mip_str}"
                 ).format(out_cvname=out_cvname, cc_cvname=cc_cvname,
                          proc_dir_path=proc_dir_path, cc_thresh=cc_thresh,
                          sz_thresh=sz_thresh, cc_dil_param=cc_dil_param,
                          chunk_begin_str=chunk_begin_str,
                          chunk_end_str=chunk_end_str, mip_str=str(mip)),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )

def merge_ccs(dag):
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_ccs",
        command=("merge_ccs {proc_dir_path} {sz_thresh}"
                      ).format(proc_dir_path=proc_dir_path, sz_thresh=sz_thresh),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )

def remap_ids(dag, chunk_begin, chunk_end):
    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="remap_ids" + "_".join(map(str,chunk_begin)),
        command=("remap_ids {cc_cvname} {cc_cvname} {proc_dir_path} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str} " +
		      "--mip {mip_str}"
                      ).format(cc_cvname=cc_cvname, proc_dir_path=proc_dir_path,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str, mip_str=str(mip)),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )

def chunk_overlaps(dag, chunk_begin, chunk_end):
    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        task_id="chunk_overlaps"+ "_".join(map(str,chunk_begin)),
        mount_point="/root/.cloudvolume/secrets",
        command=("chunk_overlaps {seg_cvname} {base_seg_cvname} {proc_dir_path} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str} --mip {mip}"
                      ).format(seg_cvname=cc_cvname, base_seg_cvname=seg_cvname,
                               proc_dir_path=proc_dir_path,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str, mip=str(mip)),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )

def merge_overlaps(dag):
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json"],
        task_id="merge_overlaps",
        mount_point="/root/.cloudvolume/secrets",
        command=("merge_overlaps {proc_dir_path} "
                      ).format(proc_dir_path=proc_dir_path),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        #queue="cpu",
        dag=dag
        )


# STEP 1: chunk_ccs
bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord)
step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]

# STEP 2: merge_ccs
step2 = merge_ccs(dag)
for chunk in step1:
    chunk.set_downstream(step2)

# STEP 3: remap_ids
step3 = [remap_ids(dag, bb[0], bb[1]) for bb in bboxes]
# using the last remap task as an intermediate node to keep dag consistent
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
