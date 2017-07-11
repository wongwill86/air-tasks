from airflow.plugins_manager import AirflowPlugin
from airflow.models import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.subdag_operator import SubDagOperator
import logging
import json

DEFAULT_IMAGE_ID = \
    '098703261575.dkr.ecr.us-east-1.amazonaws.com/chunkflow'
DEFAULT_VERSION = 'v1.7.8'


def create_chunkflow_subdag(parent_dag_name, child_dag_name, subdag_args,
                            tasks_filename):
    chunkflow_subdag = DAG(dag_id='%s.%s' % (parent_dag_name,
                                             child_dag_name),
                           default_args=subdag_args,
                           schedule_interval=None)

    with open(tasks_filename, "r") as tasks_file:
        line = tasks_file.readline()
        # skip lines that are irrelevant
        while line.strip():
            line = tasks_file.readline()
            if tasks_file.readline().startswith("PRINT TASK JSONS"):
                break

        task_json = tasks_file.readline()

        while task_json.strip():
            try:
                task = json.loads(task_json)
                task_origin = task['input']['params']['origin']
            except KeyError:
                logger = logging.getLogger(__name__)
                logger.error("Unable to find chunk input origin key " +
                             "(json.input.params.origin) from \n %s", task_json)
                raise
            except ValueError:
                logger = logging.getLogger(__name__)
                logger.error("Unable to parse task as json: \n %s", task_json)
                raise
            print task_origin
            # print task_input_params
            print('chunkflow_' + '_'.join(str(x) for x in task_origin))
            ChunkFlowOperator(
                task_id='%s-task-%s' % (child_dag_name,
                                        '_'.join(str(x) for x in task_origin)),
                task_json=task_json, dag=chunkflow_subdag)
            task_json = tasks_file.readline()

    return chunkflow_subdag


class ChunkFlowOperator(DockerOperator):
    def __init__(self,
                 image_id=DEFAULT_IMAGE_ID,
                 image_version=DEFAULT_VERSION,
                 task_json="{}",
                 *args, **kwargs
                 ):

        super(ChunkFlowOperator, self).__init__(
            image=image_id + ':' + image_version,
            command='julia ~/.julia/v0.5/ChunkFlow/scripts/main.jl -t ' +
            task_json.replace('"', '\"'),
            network_mode='bridge',
            *args, **kwargs)


class ChunkFlowTasksFileOperator(SubDagOperator):
    def __init__(self, task_id, tasks_filename,
                 image_id=DEFAULT_IMAGE_ID, version=DEFAULT_VERSION,
                 *args, **kwargs):

        subdag = create_chunkflow_subdag(
            kwargs['dag'].dag_id, task_id,
            {} if 'default_args' not in kwargs else kwargs['default_args'],
            tasks_filename)

        # SubDagOperator.downstream_list
        super(ChunkFlowTasksFileOperator, self).__init__(
            task_id=task_id,
            subdag=subdag,
            *args, **kwargs)


class ChunkFlowPlugin(AirflowPlugin):
    name = "chunkflow_plugin"
    operators = [ChunkFlowOperator, ChunkFlowTasksFileOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
