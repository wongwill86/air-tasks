from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.docker_plugin import DockerWithVariablesOperator

DAG_ID = 'synaptor_basil2_3'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018,6,29),
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

#NOTE TO SELF: EXPAND 125-140 ASYNET bboxes to 125-143 (but not the cleft ones)
#NOTE TO SELF: Z-WIDTH of chunks should be 15 (done), offset = (XX,YY,5) (done)
# =============
# run-specific args
img_cvname = "gs://neuroglancer/basil_v0/son_of_alignment/v3.04_cracks_only_normalized_rechunked"
out_cvname = "gs://neuroglancer/basil_v0/synapticmap/mip1_d2_1175k"
cleft_cvname = "gs://neuroglancer/basil_v0/clefts"

#Seg layers for different bboxes
#(25k,20k,140)<->(165k,250k,940)
seg0_cvname = "gs://neuroglancer/basil_v0/basil_full/seg-aug"
ws0_cvname = "gs://neuroglancer/basil_v0/basil_full/ws-aug"
#(173k,20k,140)<->(165k,250k,940)
seg1_cvname = "gs://neuroglancer/basil_v0/basil_full/seg-extra-aug-1"
ws1_cvname = "gs://neuroglancer/basil_v0/basil_full/ws-extra-aug-1"
##(173k,20k,8)<->(165k,250k,124)
seg2_cvname = "gs://neuroglancer/basil_v0/basil_full/seg-extra-aug-2"
ws2_cvname = "gs://neuroglancer/basil_v0/basil_full/ws-extra-aug-2"
##(25k,20k,8)<->(165k,250k,124)
seg3_cvname = "gs://neuroglancer/basil_v0/basil_full/seg-extra-aug-3"
ws3_cvname = "gs://neuroglancer/basil_v0/basil_full/ws-extra-aug-3"
#(165k,20k,**8**)<->(173k,250k,**940**)
# NOTE: need to split this one into 3 groups of bboxes to keep the grid "consistent"
seg4_cvname = "gs://neuroglancer/basil_v0/basil_full/seg-extra-aug-4"
ws4_cvname = "gs://neuroglancer/basil_v0/basil_full/ws-extra-aug-4"
##(25k,20k,125)<->(165k,250k,940)
seg5_cvname = "gs://neuroglancer/basil_v0/basil_full/seg-extra-aug-5"
ws5_cvname = "gs://neuroglancer/basil_v0/basil_full/ws-extra-aug-5"
##(25k,20k,140)<->(165k,250k,940)
seg6_cvname = "gs://neuroglancer/basil_v0/basil_full/seg-extra-aug-6"
ws6_cvname = "gs://neuroglancer/basil_v0/basil_full/ws-extra-aug-6"

# DEFINING BBOXES LATER (at bottom)
max_face_shape = (1000,1000)

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

proc_dir_path = "gs://seunglab/nick/basil"
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


def asynet_pass(dag, chunk_begin, chunk_end, seg_cvname, wshed_cvname):

    chunk_begin_str = " ".join(map(str,chunk_begin))
    chunk_end_str = " ".join(map(str,chunk_end))
    patchsz_str = " ".join(map(str,patch_sz))

    return DockerWithVariablesOperator(
        ["google-secret.json","aws-secret.json"],
        host_args={"runtime": "nvidia"},
        mount_point="/root/.cloudvolume/secrets",
        task_id="chunk_edges" + "_".join(map(str,chunk_begin)),
        command=("chunk_edges {img_cvname} {cleft_cvname} {seg_cvname} " +
                      "{proc_dir_path} {num_samples} {dil_param} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str} " +
                      "--wshed_cvname {wshed_cvname} " +
                      "--patchsz {patchsz_str} " +
                      "--img_mip {img_mip} " +
                      "--seg_mip {seg_mip} " +
                      "--mip {mip} "
                      ).format(img_cvname=img_cvname, cleft_cvname=cleft_cvname,
                               seg_cvname=seg_cvname, wshed_cvname=wshed_cvname,
                               num_samples=num_samples,
                               dil_param=asyn_dil_param,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str,
                               patchsz_str=patchsz_str,
                               proc_dir_path=proc_dir_path,
                               img_mip=img_mip, seg_mip=seg_mip, mip=asynet_mip),
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
        command=("remap_ids {cleft_cvname} {cleft_cvname} {proc_dir_path} " +
                      "--chunk_begin {chunk_begin_str} " +
                      "--chunk_end {chunk_end_str}"
                      ).format(cleft_cvname=cleft_cvname,
                               proc_dir_path=proc_dir_path,
                               chunk_begin_str=chunk_begin_str,
                               chunk_end_str=chunk_end_str),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )

# =============
# BBOX DEFINITION
# This is much more complicated than usual since we're processing
# the segmentation in 7 different bounding boxes
# bboxes0 = chunk_bboxes((140000,230000,810),(2000,2000,810),offset=(25000,20000,140), mip=mip)
# bboxes1 = chunk_bboxes((28000,230000,810),(2000,2000,810),offset=(173000,20000,140), mip=mip)
# bboxes2 = chunk_bboxes((28000,230000,120),(2000,2000,120),offset=(173000,20000,5), mip=mip)
# bboxes3 = chunk_bboxes((140000,230000,120),(2000,2000,120),offset=(25000,20000,5), mip=mip)
# #This one is especially complicated since we need the grid to be "consistent"
# bboxes4 = (chunk_bboxes((8000,230000,120),(2000,2000,120),offset=(165000,20000,5), mip=mip) +
#            chunk_bboxes((8000,230000,15),(2000,2000,15),offset=(165000,20000,125), mip=mip) +
#            chunk_bboxes((8000,230000,810),(2000,2000,810),offset=(165000,20000,140), mip=mip))
# bboxes5 = chunk_bboxes((140000,230000,15),(2000,2000,15),offset=(25000,20000,125), mip=mip)
# bboxes6 = chunk_bboxes((28000,230000,15),(2000,2000,15),offset=(173000,20000,125), mip=mip)

#ASYNET BBOXES
a_mip = asynet_mip+mip
a_bboxes0 = chunk_bboxes((140000,230000,810),(2000,2000,810),offset=(25000,20000,140), mip=a_mip)
a_bboxes1 = chunk_bboxes((28000,230000,810),(2000,2000,810),offset=(173000,20000,140), mip=a_mip)
a_bboxes2 = chunk_bboxes((28000,230000,120),(2000,2000,120),offset=(173000,20000,5), mip=a_mip)
a_bboxes3 = chunk_bboxes((140000,230000,120),(2000,2000,120),offset=(25000,20000,5), mip=a_mip)
#This one is especially complicated since we need the grid to be "consistent"
a_bboxes4 = (chunk_bboxes((8000,230000,120),(2000,2000,120),offset=(165000,20000,5), mip=a_mip) +
             chunk_bboxes((8000,230000,18),(2000,2000,18),offset=(165000,20000,125), mip=a_mip) +
             chunk_bboxes((8000,230000,810),(2000,2000,810),offset=(165000,20000,140), mip=a_mip))
a_bboxes5 = chunk_bboxes((140000,230000,18),(2000,2000,18),offset=(25000,20000,125), mip=a_mip)
a_bboxes6 = chunk_bboxes((28000,230000,18),(2000,2000,18),offset=(173000,20000,125), mip=a_mip)

# cpu_bboxes = bboxes0 + bboxes1 + bboxes2 + bboxes3 + bboxes4 + bboxes5 + bboxes6
# bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord, mip=mip)
# asynet_bboxes = chunk_bboxes(vol_shape, chunk_shape,
                            #  offset=start_coord, mip=asynet_mip+mip)
# # =============
# #PART 1: chunk_ccs and merge_ccs for all bboxes
#
# step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in cpu_bboxes]
#
# step2 = merge_ccs(dag)
# for chunk in step1:
#     chunk.set_downstream(step2)
#
# #NOTE TO SELF: NEED TO COPY CLEFT 15-SLICE ID MAPS TO MATCH 18-SLICE CHUNKS
# # BEFORE RUNNING step3_[4-6]
#
# # =============
# #PART 2: chunk_edges for all bboxes by respective segmentation, and merging
#
# # STEP 3: asynet pass
# step3_0 = [asynet_pass(dag, bb[0], bb[1], seg0_cvname, ws0_cvname) for bb in a_bboxes0]
# step3_1 = [asynet_pass(dag, bb[0], bb[1], seg1_cvname, ws1_cvname) for bb in a_bboxes1]
# step3_2 = [asynet_pass(dag, bb[0], bb[1], seg2_cvname, ws2_cvname) for bb in a_bboxes2]
step3_3 = [asynet_pass(dag, bb[0], bb[1], seg3_cvname, ws3_cvname) for bb in a_bboxes3]
# step3_4 = [asynet_pass(dag, bb[0], bb[1], seg4_cvname, ws4_cvname) for bb in a_bboxes4]
# step3_5 = [asynet_pass(dag, bb[0], bb[1], seg5_cvname, ws5_cvname) for bb in a_bboxes5]
# step3_6 = [asynet_pass(dag, bb[0], bb[1], seg6_cvname, ws6_cvname) for bb in a_bboxes6]
#
# #Might not ever use this, likely running each segment on its own
# #step3 = step3_0 + step3_1 + step3_2 + step3_3 + step3_4 + step3_5 + step3_6
#
# # for chunk in step3:
# #     step2.set_downstream(chunk)
#
# #NOTE TO SELF: NEED TO COPY 18-SLICE CHUNKS BACK TO 15-SLICE CHUNKS BEFORE
# # RUNNING PART 3
#
# # =============
# #PART 3: merge_edges across dataset -> remapping
#
# step4 = merge_edges(dag)
# # for chunk in step3:
# #     chunk.set_downstream(step4)
#
# # STEP 5: remap_ids
# step5 = [remap_ids(dag, bb[0], bb[1]) for bb in cpu_bboxes]
# for chunk in step5:
#     step4.set_downstream(chunk)
