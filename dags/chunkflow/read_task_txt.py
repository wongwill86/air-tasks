# import json
# import logging
# import re
# from airflow.operators.docker_operator import DockerOperator
# from airflow import DAG
# from datetime import datetime, timedelta
# import string
# logging.basicConfig(level=logging.INFO)


# class ChunkflowOperator(DockerOperator):

# def create_chunkflow_task(name, task_text, version='v1.7.8',
#         image_id=DEFAULT_CHUNKFLOW_IMAGE_ID):
#     return DockerOperator(
#             task_id=name'watershed_print_' + '_'.join(str(x) for x in
#                 task_origin),
#             image=image_id + ':' + version,
#             command='julia ~/.julia/v0.5/Chunkflow/scripts/main.jl -t ' +
#                 task,
#             command='julia -e \'print("json is ' + re.escape(line) + '!")\'',
#             network_mode='bridge',
#             )

# def create_chunk_tasks(filename, chunkflow_image):
#     with open(filename, "r") as task_file:
#         line = task_file.readline()
#         while line:
#             line = task_file.readline()
#             if task_file.readline().startswith("PRINT TASK JSONS"):
#                 break
#         while line.strip():
#             task_json = task_file.readline()
#             try:
#                 task = json.loads(task_json)
#                 task_origin = task['input']['params']['origin']
#             except KeyError:
#                 logger = logging.getLogger(__name__)
#                 logger.error("Unable to find chunk input origin key "
#                       "(json.input.params.origin) from \n %s", task_json)
#                 raise
#             except ValueError:
#                 logger = logging.getLogger(__name__)
#                 logger.error("Unable to parse task as json: \n %s", task_json)
#             print task_origin
#             line = task_file.readline()
#             print task_input_params
#             print('chunkflow_' + '_'.join(str(x) for x in task_origin))

# filename = "./dags/chunkflow/tasks.txt"
# print(create_chunk_tasks(filename, ))
