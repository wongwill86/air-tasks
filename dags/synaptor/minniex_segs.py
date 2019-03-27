from airflow import DAG
from datetime import datetime, timedelta
# from airflow.operators.docker_plugin import DockerWithVariablesOperator
from airflow.operators.docker_plugin import DockerWithVariablesMultiMountOperator  # noqa
from airflow.operators.bash_operator import BashOperator

import itertools
import operator

DAG_ID = 'minniex_segs'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 3, 27),
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
img_cvname = "gs://microns-seunglab/minnie65/single_sections"
seg_cvname = "gs://microns-seunglab/ranl/minnie65/seg_minnie65_small_0"
out_cvname = "gs://microns-seunglab/minnie65/psd"
cleft_cvname = "gs://microns-seunglab/minnie65/clefts/minniex_segs_temp"
cleft_out_cvname = "gs://microns-seunglab/minnie65/clefts/minniex_segs"

# VOLUME COORDS
start_coord = (63489, 87557, 19999)
vol_shape = (364300, 251952, 2046)
chunk_shape = (2080, 2080, 1023)
max_face_shape = (1040, 1040)

mip = 1
base_res = (8, 8, 40)
asynet_res = (8, 8, 40)

cc_thresh = 0.27
sz_thresh1 = 25
sz_thresh2 = 100

num_samples = 1
asyn_dil_param = 5
patch_sz = (80, 80, 18)
num_downsamples = 0

dist_thr = 700

proc_url = "PROC_FROM_FILE"
proc_dir = "gs://microns-seunglab/nick/minniex_segtest"
hashmax = 400
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
