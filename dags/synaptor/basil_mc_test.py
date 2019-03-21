from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.docker_plugin import DockerWithVariablesMultiMountOperator  # noqa
from airflow.operators.bash_operator import BashOperator

import itertools
import operator

DAG_ID = 'basil_mc_test'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 11),
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
img_cvname = "gs://neuroglancer/pinky100_v0/son_of_alignment_v15_rechunked"
seg_cvname = "gs://neuroglancer/pinky100_v0/seg/lost_no-random/bbox1_0"
out_cvname = "gs://neuroglancer/pinky100_v0/psd/mip1_d2_1175k"
cleft_cvname = "gs://neuroglancer/pinky100_v0/cleft_tests/mip1_d2_1175k_temp"
cleft_out_cvname = "gs://neuroglancer/pinky100_v0/cleft_tests/mip1_d2_1175k"

# VOLUME COORDS
start_coord = (36192, 30558, 21)
vol_shape = (86016, 51200, 2136)
chunk_shape = (2048, 2048, 1068)
max_face_shape = (1068, 1068)

mip = 1
base_res = (8, 8, 40)
asynet_res = (8, 8, 40)

cc_thresh = 0.26
sz_thresh1 = 25
sz_thresh2 = 50

num_samples = 1
asyn_dil_param = 5
patch_sz = (80, 80, 18)
num_downsamples = 0

dist_thr = 500

proc_url = "PROC_FROM_FILE"
proc_dir = "gs://seunglab/nick/basil_scale_test"
hashmax = 25
# =============
# Helper functions


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
    task_tag = "chunk_ccs_" + "_".join(map(str, chunk_begin))

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"chunk_ccs {out_cvname} {cleft_cvname} {proc_url}"
                 f" {cc_thresh} {sz_thresh1}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --mip {res_str} --proc_dir {proc_dir}"
                 f" --hashmax {hashmax}"
                 f" --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def match_contins(dag, i):

    faceshape_str = tup2str(max_face_shape)
    task_tag = f"match_contins_{i}"

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"match_contins {proc_url} {i}"
                 f" --max_face_shape {faceshape_str}"
                 f" --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def seg_graph_ccs(dag):

    task_tag = "seg_graph_ccs"
    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"seg_graph_ccs {proc_url} {hashmax}"
                 f" --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def chunk_seg_map(dag):

    task_tag = "chunk_seg_map"
    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"chunk_seg_map {proc_url} --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def merge_seginfo(dag, i):

    task_tag = f"merge_seginfo_{i}"

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"merge_seginfo {proc_url} {i}"
                 f" --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def chunk_edges(dag, chunk_begin, chunk_end, base_begin, base_end):

    chunk_begin_str = tup2str(chunk_begin)
    chunk_end_str = tup2str(chunk_end)
    base_begin_str = tup2str(base_begin)
    base_end_str = tup2str(base_end)
    patchsz_str = tup2str(patch_sz)
    res_str = tup2str(asynet_res)

    task_tag = "chunk_edges" + "_".join(map(str, chunk_begin))

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        host_args={"runtime": "nvidia"},
        task_id=task_tag,
        command=(f"chunk_edges {img_cvname} {cleft_cvname} {seg_cvname}"
                 f" {proc_url} {hashmax}"
                 f" --samples_per_cleft {num_samples}"
                 f" --dil_param {asyn_dil_param}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --patchsz {patchsz_str} --resolution {res_str}"
                 f" --num_downsamples {num_downsamples}"
                 f" --base_res_begin {base_begin_str}"
                 f" --base_res_end {base_end_str}"
                 f" --proc_dir {proc_dir}"
                 f" --resolution {res_str}"
                 f" --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="gpu",
        dag=dag
        )


def pick_edge(dag, i):

    task_tag = f"pick_edge_{i}"

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"pick_edge {proc_url} {i}"
                 f" --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def merge_dups(dag, i):

    res_str = " ".join(map(str, base_res))

    task_tag = f"merge_dups_{i}"

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"merge_dups {proc_url} {i} {dist_thr} {sz_thresh2}"
                 f" --voxel_res {res_str}"
                 f" --timing_tag {task_tag}"
                 f" --fulldf_proc_url {proc_dir}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def remap_ids(dag, chunk_begin, chunk_end):

    chunk_begin_str = tup2str(chunk_begin)
    chunk_end_str = tup2str(chunk_end)
    res_str = tup2str(base_res)

    task_tag = "remap_ids" + "_".join(map(str, chunk_begin))

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"remap_ids {cleft_cvname} {cleft_out_cvname} {proc_url}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --mip {res_str}"
                 f" --timing_tag {task_tag}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def end_step(dag, i):
    return BashOperator(
        task_id=f"end_{i}_step",
        bash_command="date",
        queue="cpu",
        dag=dag)


#bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord, mip=mip)
#asynet_bboxes = chunk_bboxes(vol_shape, chunk_shape,
#                             offset=start_coord, mip=mip)

## STEP 1: CHUNK CONNECTED COMPONENTS
#cc_step = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]
#
#end_cc_step = end_step(dag, "chunk_ccs")
#end_cc_step.set_upstream(cc_step)


# STEP 2: MATCH CONTINUATIONS
match_contins_step = [match_contins(dag, i) for i in range(hashmax)]

#end_cc_step.set_downstream(match_contins_step)
#
#
## STEP 3: SEGMENT GRAPH CONNECTED COMPONENTS
#seg_graph_cc_step = seg_graph_ccs(dag)
#
#seg_graph_cc_step.set_upstream(match_contins_step)
#
#
#
## STEP 4: CHUNKING SEGMENTATION ID MAP
#chunk_seg_map_step = chunk_seg_map(dag)
#
#chunk_seg_map_step.set_upstream(seg_graph_cc_step)
#
#
## STEP 5: MERGING SEGMENT INFO
#merge_seginfo_step = [merge_seginfo(dag, i) for i in range(hashmax)]
#end_merge_seginfo_step = end_step(dag, "merge_seginfo")
#
#chunk_seg_map_step.set_downstream(merge_seginfo_step)
#end_merge_seginfo_step.set_upstream(merge_seginfo_step)
#
#
## STEP 6: CHUNK EDGE ASSIGNMENT
## random.seed(99999)
## random.sample(range(len(bboxes)), 170)
#indices = [493, 1266, 2048, 974, 1454, 1694, 829, 1038, 1660, 828, 245, 1364,
#           1574, 1123, 1144, 2078, 717, 694, 1317, 861, 1291, 353, 586, 515,
#           1151, 667, 2096, 1002, 1682, 2061, 569, 1614, 1657, 246, 77, 47,
#           1723, 367, 238, 1244, 139, 1422, 110, 396, 1627, 1255, 135, 322,
#           1062, 1832, 789, 1929, 168, 1298, 741, 1415, 1818, 516, 1109, 1761,
#           852, 1358, 434, 53, 1203, 419, 1338, 1494, 175, 1608, 311, 199,
#           1583, 979, 1591, 2006, 1172, 1954, 1684, 416, 920, 1975, 815, 1520,
#           746, 300, 1471, 930, 191, 1025, 1587, 877, 1110, 444, 1441, 745,
#           567, 792, 236, 308, 1377, 429, 1898, 1408, 1340, 962, 738, 1998,
#           1786, 554, 31, 385, 1149, 425, 1991, 326, 32, 703, 1530, 1170, 873,
#           1401, 344, 127, 1434, 1246, 2024, 1329, 1353, 298, 1745, 1985, 105,
#           681, 1681, 372, 1245, 1313, 1552, 178, 1606, 766, 960, 2009, 605,
#           455, 1452, 767, 711, 2049, 1675, 1863, 876, 1429, 1696, 1378, 584,
#           1654, 1481, 710, 836, 801, 291, 1289, 1419, 941, 1435, 592, 376,
#           1776]
#bbox_sample = [bboxes[i] for i in indices]
#asynet_bbox_sample = [asynet_bboxes[i] for i in indices]
#
#chunk_edges_step = [chunk_edges(dag, abb[0], abb[1], bb[0], bb[1])
#                    for (abb, bb) in zip(asynet_bbox_sample, bbox_sample)]
#
#end_merge_seginfo_step.set_downstream(chunk_edges_step)
