from airflow.plugins_manager import AirflowPlugin
from airflow.models import DAG
from airflow.operators.subdag_operator import SubDagOperator
import logging
import json
from plugins.custom.docker import DockerRemovableContainer


class ChunkFlowOperator(DockerRemovableContainer):
    DEFAULT_IMAGE_ID = '098703261575.dkr.ecr.us-east-1.amazonaws.com/chunkflow'
    DEFAULT_VERSION = 'v1.7.8'
    DEFAULT_COMMAND = 'julia /root/.julia/v0.5/ChunkFlow/scripts/main.jl ' + \
        '-t "%s"'

    def __init__(self,
                 image_id=DEFAULT_IMAGE_ID,
                 image_version=DEFAULT_VERSION,
                 command=DEFAULT_COMMAND,
                 task_json="{}",
                 *args, **kwargs
                 ):
        print("using " + image_id + ':' + image_version)
        super(ChunkFlowOperator, self).__init__(
            image=image_id + ':' + image_version,
            command=command % task_json.replace('"', '\\"'),
            network_mode='bridge',
            *args, **kwargs)


def create_chunkflow_subdag(parent_dag_name, child_dag_name, subdag_args,
                            tasks_filename, image_id, image_version):
    subdag_name = '%s.%s' % (parent_dag_name, child_dag_name)
    subdag = DAG(dag_id=subdag_name, default_args=subdag_args,
                 schedule_interval='@once')
    print('Generating subdag: <%s>' % subdag_name)

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
                             "(json.input.params.origin) from \n %s",
                             task_json)
                raise
            except ValueError:
                logger = logging.getLogger(__name__)
                logger.error("Unable to parse task as json: \n %s", task_json)
                raise

            print(task_origin)
            # print task_input_params
            print('chunkflow_' + '_'.join(str(x) for x in task_origin))
            print(task_json)
            ChunkFlowOperator(
                task_id='%s-task-%s' % (child_dag_name,
                                        '_'.join(str(x) for x in task_origin)),
                task_json=task_json, dag=subdag,
                image_id=image_id,
                image_version=image_version
            )
            task_json = tasks_file.readline()

    return subdag


def chunkflow_subdag_from_file(tasks_filename,
                               image_id=ChunkFlowOperator.DEFAULT_IMAGE_ID,
                               image_version=ChunkFlowOperator.DEFAULT_VERSION,
                               *args, **kwargs):
    subdag = create_chunkflow_subdag(
        kwargs['dag'].dag_id, kwargs['task_id'],
        {} if 'default_args' not in kwargs else kwargs['default_args'],
        tasks_filename,
        image_id,
        image_version
    )
    return SubDagOperator(subdag=subdag, *args, **kwargs)


class ChunkFlowPlugin(AirflowPlugin):
    name = "chunkflow_plugin"
    operators = [ChunkFlowOperator, chunkflow_subdag_from_file]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
