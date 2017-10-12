from airflow.plugins_manager import AirflowPlugin
from datetime import datetime
import logging
import types
import collections

from airflow.models import BaseOperator
from airflow.models import DagBag
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.file import TemporaryDirectory
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow import settings
# from docker import APICLient as Client


class MultiTriggerDagRunOperator(BaseOperator):
    """
    Triggers multiple DAG runs for a specified ``dag_id``.

    Draws inspiration from:
        airflow.operators.dagrun_operator.TriggerDagRunOperator

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param params_list: list of dicts for DAG level parameters that are made
        acesssible in templates
 namespaced under params for each dag run.
    :type params: Iterable<dict> or types.GeneratorType
    """

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            params_list,
            *args, **kwargs):
        super(MultiTriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.params_list = params_list
        if hasattr(self.params_list, '__len__'):
            assert len(self.params_list) > 0
        else:
            assert (isinstance(params_list, collections.Iterable) or
                    isinstance(params_list, types.GeneratorType))

    def execute(self, context):
        session = settings.Session()
        dbag = DagBag(settings.DAGS_FOLDER)
        trigger_dag = dbag.get_dag(self.trigger_dag_id)

        assert trigger_dag is not None

        trigger_id = 0
        # for trigger_id in range(0, len(self.params_list)):
        for params in self.params_list:
            dr = trigger_dag.create_dagrun(run_id='trig_%s_%d_%s' %
                                           (self.trigger_dag_id, trigger_id,
                                            datetime.now().isoformat()),
                                           state=State.RUNNING,
                                           conf=params,
                                           external_trigger=True)
            logging.info("Creating DagRun {}".format(dr))
            session.add(dr)
            trigger_id = trigger_id + 1
            if trigger_id % 10:
                session.commit()
        session.commit()
        session.close()


class DockerWithVariablesOperator(DockerOperator):
    DEFAULT_MOUNT_POINT = '/run/secrets'
    WRITER_IMAGE = 'alpine:latest'

    def __init__(self,
                 variables,
                 mount_point=DEFAULT_MOUNT_POINT,
                 *args, **kwargs):
        self.variables = variables
        self.mount_point = mount_point
        super(DockerWithVariablesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        with TemporaryDirectory(prefix='dockervariables') as tmp_var_dir:
            for key in self.variables:
                value = Variable.get(key)
                with open('{0}:{1}'.format(tmp_var_dir, key),
                          'w') as value_file:
                    value_file.write(str(value))
            self.volumes.append('{0}:{1}'.format(tmp_var_dir,
                                                 self.mount_point))
            return super(DockerWithVariablesOperator, self).execute(context)


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [MultiTriggerDagRunOperator, DockerWithVariablesOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
