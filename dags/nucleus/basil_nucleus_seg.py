from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.docker_plugin import DockerWithVariablesMultiMountOperator  # noqa
from airflow.operators.bash_operator import BashOperator

import itertools
import operator

DAG_ID = 'basil_nucleus_test'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 3),
    'cactchup_by_default': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'retry_exponential_backoff': False,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
)

# =============
# run-specific args
out_cvname = "gs://microns-seunglab/basil_v0/nucleus/v0"
seg_cvname = "gs://microns-seunglab/basil_v0/nucleus/v0/seg_temp"
seg_out_cvname = "gs://microns-seunglab/basil_v0/nucleus/v0/seg"

# VOLUME COORDS
start_coord = (-192, -192, 0)  # whole volume
vol_shape = (15488, 18560, 1024)  # whole volume
# start_coord = (113664, 140288, 0)
# vol_shape = (3096, 3096, 1024)
chunk_shape = (1024, 1024, 1024)
max_face_shape = (1024, 1024)

mip = 4
voxel_res = (64, 64, 40)

cc_thresh = 0.5
sz_thresh1 = 250
sz_thresh2 = 1000

proc_url = "PROC_FROM_FILE"
proc_dir = "gs://microns-seunglab/shang/basil_seg_test"
hashmax = 1
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
    res_str = tup2str(voxel_res)
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
        queue="graph",
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
        queue="dbmessenger",
        dag=dag
        )


def create_index(dag, tablename, colname):

    task_tag = f"create_index_{tablename}_{colname}"
    return DockerWithVariablesMultiMountOperator(
        ["google-secret.json", "proc_url", "boto"],
        mount_points=["/root/.cloudvolume/secrets/google-secret.json",
                      "/root/proc_url", "/root/.boto"],
        task_id=task_tag,
        command=(f"create_index {proc_url} {tablename} {colname}"),
        default_args=default_args,
        image="seunglab/synaptor:latest",
        queue="dbmessenger",
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


def remap_ids(dag, chunk_begin, chunk_end):

    chunk_begin_str = tup2str(chunk_begin)
    chunk_end_str = tup2str(chunk_end)
    res_str = tup2str(voxel_res)

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


bboxes = chunk_bboxes(vol_shape, chunk_shape, offset=start_coord, mip=mip)

# STEP 1: CHUNK CONNECTED COMPONENTS
cc_step = [chunk_ccs(dag, bb[0], bb[1]) for bb in bboxes]

end_cc_step = end_step(dag, "chunk_ccs")
end_cc_step.set_upstream(cc_step)


# STEP 2: MATCH CONTINUATIONS
match_contins_step = [match_contins(dag, i) for i in range(hashmax)]

end_cc_step.set_downstream(match_contins_step)


# STEP 3: SEGMENT GRAPH CONNECTED COMPONENTS
seg_graph_cc_step = seg_graph_ccs(dag)

seg_graph_cc_step.set_upstream(match_contins_step)



# STEP 4: CREATING INDEX FOR DST_ID_HASH
dst_id_hash_idx_step = create_index(dag, "seg_merge_map", "dst_id_hash")

dst_id_hash_idx_step.set_upstream(seg_graph_cc_step)



# STEP 5: CHUNKING SEGMENTATION ID MAP
chunk_seg_map_step = chunk_seg_map(dag)

chunk_seg_map_step.set_upstream(dst_id_hash_idx_step)


# STEP 6: CREATING INDEX FOR CHUNK_TAG
chunk_tag_idx_step = create_index(dag, "chunked_seg_merge_map", "chunk_tag")

chunk_tag_idx_step.set_upstream(chunk_seg_map_step)



# STEP 7: MERGING SEGMENT INFO
merge_seginfo_step = [merge_seginfo(dag, i) for i in range(hashmax)]
end_merge_seginfo_step = end_step(dag, "merge_seginfo")

chunk_tag_idx_step.set_downstream(merge_seginfo_step)
end_merge_seginfo_step.set_upstream(merge_seginfo_step)


# STEP 8: REMAP IDS
remap_step = [remap_ids(dag, bb[0], bb[1]) for bb in bboxes]

end_merge_seginfo_step.set_downstream(remap_step)
