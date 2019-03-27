from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.docker_plugin import DockerWithVariablesMultiMountOperator  # noqa

import itertools
import operator

DAG_ID = 'stitched_test'

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
img_cvname = "gs://neuroglancer/pinky_training/stitched.img"
seg_cvname = "gs://neuroglancer/pinky_training/stitched.seg"
out_cvname = "gs://neuroglancer/nick/pinky/stitched/mip1_d2_1175k_output"
cleft_cvname = "gs://neuroglancer/nick/synaptor_test/stitched_test"
cleft_cvname2 = "gs://neuroglancer/nick/synaptor_test/stitched_test2"

# VOLUME COORDS
start_coord = (0, 0, 0)
vol_shape = (1024, 1024, 100)
chunk_shape = (512, 512, 50)
max_face_shape = (512, 512)

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
proc_dir = "gs://seunglab/nick/stitched_test"
hashmax = 10
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

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id="chunk_ccs_" + "_".join(map(str, chunk_begin)),
        command=(f"chunk_ccs {out_cvname} {cleft_cvname} {proc_url}"
                 f" {cc_thresh} {sz_thresh1}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --mip {res_str} --proc_dir {proc_dir}"
                 f" --hashmax {hashmax}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def match_contins(dag, i):

    faceshape_str = tup2str(max_face_shape)

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=f"match_contins_{i}",
        command=(f"match_contins {proc_url} {i}"
                 f" --max_face_shape {faceshape_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def seg_graph_ccs(dag):

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=f"seg_graph_ccs",
        command=(f"seg_graph_ccs {proc_url} {hashmax}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def merge_seginfo(dag, i):

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=f"merge_seginfo_{i}",
        command=(f"merge_seginfo {proc_url} {i}"),
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

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        host_args={"runtime": "nvidia"},
        task_id="chunk_edges" + "_".join(map(str, chunk_begin)),
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
                 f" --proc_dir {proc_dir}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="gpu",
        dag=dag
        )


def pick_edge(dag, i):

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=f"pick_edge_{i}",
        command=(f"pick_edge {proc_url} {i}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


def merge_dups(dag, i):

    res_str = " ".join(map(str, base_res))

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=f"merge_dups_{i}",
        command=(f"merge_dups {proc_url} {i} {dist_thr} {sz_thresh2}"
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

    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id="remap_ids" + "_".join(map(str, chunk_begin)),
        command=(f"remap_ids {cleft_cvname} {cleft_cvname2} {proc_url}"
                 f" --chunk_begin {chunk_begin_str}"
                 f" --chunk_end {chunk_end_str}"
                 f" --mip {res_str}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="cpu",
        dag=dag
        )


bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord)
asynet_bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord)

step1 = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]
for task in step1[:-1]:
    task.set_downstream(step1[-1])

step2 = [match_contins(dag, i) for i in range(hashmax)]
for task in step2[:-1]:
    task.set_upstream(step1[-1])
    task.set_downstream(step2[-1])

step3 = seg_graph_ccs(dag)
step3.set_upstream(step2[-1])

step4 = [merge_seginfo(dag, i) for i in range(hashmax)]
for task in step4[:-1]:
    task.set_upstream(step3)
    task.set_downstream(step4[-1])

step5 = [chunk_edges(dag, abb[0], abb[1], bb[0], bb[1])
         for (abb, bb) in zip(asynet_bboxes, bboxes)]
for task in step5[:-1]:
    task.set_upstream(step4[-1])
    task.set_downstream(step5[-1])

step6 = [pick_edge(dag, i) for i in range(hashmax)]
for task in step6[:-1]:
    task.set_upstream(step5[-1])
    task.set_downstream(step6[-1])

step7 = [merge_dups(dag, i) for i in range(hashmax)]
for task in step7[:-1]:
    task.set_upstream(step6[-1])
    task.set_downstream(step7[-1])

step8 = [remap_ids(dag, bb[0], bb[1]) for bb in bboxes]
for task in step8:
    task.set_upstream(step7[-1])
