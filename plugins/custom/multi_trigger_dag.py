from airflow.plugins_manager import AirflowPlugin
from datetime import datetime
import logging

from airflow.models import BaseOperator
from airflow.models import DagBag
from airflow.utils.decorators import apply_defaults
from airflow.utils.state import State
from airflow import settings


class MultiTriggerDagRunOperator(BaseOperator):
    """
    Triggers multiple DAG runs for a specified ``dag_id``.

    Draws inspiration from:
        airflow.operators.dagrun_operator.TriggerDagRunOperator

    :param trigger_dag_id: the dag_id to trigger
    :type trigger_dag_id: str
    :param param_list: list of dicts for DAG level parameters that are made
        acesssible in templates
 namespaced under params for each dag run.
    :type params: list of dicts
    """

    @apply_defaults
    def __init__(
            self,
            trigger_dag_id,
            param_list,
            *args, **kwargs):
        super(MultiTriggerDagRunOperator, self).__init__(*args, **kwargs)
        self.trigger_dag_id = trigger_dag_id
        self.param_list = param_list
        assert len(self.param_list) > 0

    def execute(self, context):
        session = settings.Session()
        dbag = DagBag(settings.DAGS_FOLDER)
        trigger_dag = dbag.get_dag(self.trigger_dag_id)

        assert trigger_dag is not None

        for trigger_id in range(0, len(self.param_list)):
            dr = trigger_dag.create_dagrun(run_id='trig_%s_%d_%s' %
                                           (self.trigger_dag_id, trigger_id,
                                            datetime.now().isoformat()),
                                           state=State.RUNNING,
                                           conf=self.param_list[trigger_id],
                                           external_trigger=True)
            logging.info("Creating DagRun {}".format(dr))
            session.add(dr)
        session.commit()
        session.close()


class CustomPlugin(AirflowPlugin):
    name = "custom_plugin"
    operators = [MultiTriggerDagRunOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
