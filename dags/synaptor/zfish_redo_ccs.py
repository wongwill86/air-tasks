# Basic demonstration of how to get nvidia-docker running
# This will only work with nvidida-drivers >= 367.48 see
# https://github.com/NVIDIA/nvidia-docker/wiki/CUDA for compatibility matrix
# This only works if you have set up `apt-get nvidia-docker2`

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator


DAG_ID = 'synaptor_redo_ccs'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 12),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

# =============
# run-specific args
img_cvname = "gs://neuroglancer/zfish_v1/image"
seg_cvname = "gs://neuroglancer/zfish_v1/consensus-20171130"
out_cvname = "gs://neuroglancer/zfish_v1/psd"
cc_cvname = "gs://neuroglancer/zfish_v1/clefts"

# FULL VOLUME COORDS
start_coord = (14336, 12288, 16384)
vol_shape   = (69632, 32768, 1792)
chunk_shape = (1024,1024,1792)

# TEST VOLUME COORDS
#start_coord = (52736, 24576, 17344)
#vol_shape = (3072, 2048, 256)
#chunk_shape = (1024, 1024, 128)

cc_thresh = 0.1
sz_thresh = 200
cc_dil_param = 0

num_samples = 2
asyn_dil_param = 5
patch_sz = (160, 160, 18)

voxel_res = (5, 5, 45)
dist_thr = 1000

proc_dir_path = "gs://seunglab/nick/zfish_full"
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
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_ccs_" + "_".join(map(str,chunk_begin)),
        command=("chunk_ccs {out_cvname} {cc_cvname} {proc_dir_path} " +
                 "{cc_thresh} {sz_thresh} {cc_dil_param} " +
                 "--chunk_begin {chunk_begin_str} " +
                 "--chunk_end {chunk_end_str}"
                 ).format(out_cvname=out_cvname, cc_cvname=cc_cvname,
                          proc_dir_path=proc_dir_path, cc_thresh=cc_thresh,
                          sz_thresh=sz_thresh, cc_dil_param=cc_dil_param,
                          chunk_begin_str=chunk_begin_str,
                          chunk_end_str=chunk_end_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def merge_ccs(dag):
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_ccs",
        command=("merge_ccs {proc_dir_path} {sz_thresh}"
                      ).format(proc_dir_path=proc_dir_path, sz_thresh=sz_thresh),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def asynet_pass(dag, chunk_begin, chunk_end):
    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    patchsz_str = " ".join(map(str,patch_sz))
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        host_args={"runtime": "nvidia"},
        mount_point="/root/.cloudvolume/secrets",
        task_id="asynet_pass" + "_".join(map(str,chunk_begin)),
        command=("asynet_pass {img_cvname} {cc_cvname} {seg_cvname} " +
                      "{num_samples} {dil_param} {proc_dir_path} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str} " +
                      "--patchsz {patchsz_str}"
                      ).format(img_cvname=img_cvname, cc_cvname=cc_cvname,
                               seg_cvname=seg_cvname, num_samples=num_samples,
                               dil_param=asyn_dil_param,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str,
                               patchsz_str=patchsz_str,
                               proc_dir_path=proc_dir_path),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="gpu",
        dag=dag
        )


def merge_edges(dag):
    voxel_res_str = " ".join(map(str,voxel_res))
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="merge_edges",
        command=("merge_edges {proc_dir_path} {dist_thr} " +
                      "--voxel_res {voxel_res_str} "
                      ).format(proc_dir_path=proc_dir_path, dist_thr=dist_thr,
                               voxel_res_str=voxel_res_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def remap_ids(dag, chunk_begin, chunk_end):
    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    return DockerWithVariablesOperator(
        ["project_name","google-secret.json","aws-secret.json"],
        mount_point="/root/.cloudvolume/secrets",
        task_id="remap_ids" + "_".join(map(str,chunk_begin)),
        command=("remap_ids {cc_cvname} {cc_cvname} {proc_dir_path} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str}"
                      ).format(cc_cvname=cc_cvname, proc_dir_path=proc_dir_path,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


# STEP 1: chunk_ccs
begs = [(82944, 14336, 16384), (27648, 13312, 16384), (49152, 15360, 16384),
        (30720, 33792, 16384), (62464, 19456, 16384), (19456, 12288, 16384),
        (62464, 25600, 16384), (77824, 19456, 16384), (26624, 41984, 16384),
        (24576, 44032, 16384), (57344, 29696, 16384), (80896, 25600, 16384),
        (77824, 36864, 16384), (16384, 23552, 16384), (56320, 29696, 16384),
        (20480, 44032, 16384), (81920, 34816, 16384), (57344, 28672, 16384),
        (56320, 19456, 16384), (34816, 31744, 16384), (58368, 22528, 16384),
        (56320, 21504, 16384), (16384, 27648, 16384), (18432, 31744, 16384),
        (35840, 38912, 16384), (65536, 40960, 16384), (33792, 23552, 16384),
        (19456, 17408, 16384), (22528, 27648, 16384), (30720, 40960, 16384),
        (32768, 17408, 16384), (60416, 14336, 16384), (39936, 20480, 16384),
        (16384, 34816, 16384), (61440, 44032, 16384), (23552, 38912, 16384),
        (82944, 43008, 16384), (63488, 25600, 16384), (69632, 15360, 16384),
        (67584, 17408, 16384), (36864, 40960, 16384), (79872, 37888, 16384),
        (78848, 43008, 16384), (52224, 16384, 16384), (48128, 35840, 16384),
        (55296, 17408, 16384), (14336, 14336, 16384), (46080, 30720, 16384),
        (50176, 43008, 16384), (77824, 22528, 16384), (49152, 21504, 16384),
        (58368, 39936, 16384), (72704, 22528, 16384), (66560, 23552, 16384),
        (71680, 18432, 16384), (30720, 23552, 16384), (73728, 43008, 16384),
        (24576, 12288, 16384), (23552, 18432, 16384), (57344, 17408, 16384),
        (49152, 26624, 16384), (41984, 35840, 16384), (17408, 33792, 16384),
        (18432, 15360, 16384), (22528, 35840, 16384), (32768, 19456, 16384),
        (72704, 40960, 16384), (37888, 33792, 16384), (43008, 30720, 16384),
        (49152, 40960, 16384), (59392, 40960, 16384), (82944, 20480, 16384),
        (24576, 33792, 16384), (78848, 39936, 16384), (25600, 41984, 16384),
        (18432, 44032, 16384), (28672, 26624, 16384), (28672, 36864, 16384),
        (61440, 24576, 16384), (52224, 44032, 16384), (25600, 37888, 16384),
        (60416, 26624, 16384), (74752, 21504, 16384), (71680, 19456, 16384),
        (35840, 29696, 16384), (49152, 38912, 16384), (39936, 19456, 16384),
        (52224, 14336, 16384), (20480, 43008, 16384), (69632, 17408, 16384),
        (52224, 41984, 16384), (41984, 30720, 16384), (75776, 33792, 16384),
        (81920, 43008, 16384), (62464, 40960, 16384), (39936, 27648, 16384),
        (18432, 18432, 16384), (14336, 37888, 16384), (41984, 37888, 16384),
        (66560, 44032, 16384), (80896, 37888, 16384), (19456, 41984, 16384),
        (49152, 27648, 16384)]

bboxes = [ (b, tuple(map(operator.add, b,chunk_shape))) for b in begs]
step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]
