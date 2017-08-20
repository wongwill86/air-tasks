from __future__ import unicode_literals
from airflow.operators.custom_plugin import MultiTriggerDagRunOperator
from airflow import settings

from tests.utils.mock_helpers import patch_plugin_file

try:
    import unittest.mock as mock
except ImportError:
    import mock

from airflow.models import DAG
from datetime import datetime, timedelta

DAG_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}
TASK_ID = 'MultiTriggerDag'


class TestMultiTriggerDag(object):
    @staticmethod
    def create_trigger_dag(trigger_dag_id):
        return DAG(dag_id=trigger_dag_id,
                   default_args=DAG_ARGS,
                   schedule_interval=None)

    def test_create(self):
        operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                              trigger_dag_id="hello",
                                              param_list=[],
                                              default_args=DAG_ARGS)
        assert operator

    @patch_plugin_file('plugins/custom/multi_trigger_dag', 'DagBag',
                       autospec=True)
    def test_execute(self, dag_bag_class):
        a = "a"
        b = "b"
        c = "c"
        d = "d"
        params_list = [a, b, c, d]
        mock_dag_bag = mock.MagicMock(name='DagBag')
        mock_dag_bag.create_dagrun.side_effect = lambda *args, **kwargs: kwargs

        dag_bag_class.return_value = mock_dag_bag

        session = settings.Session()
        session.add = mock.MagicMock('add')

        operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                              trigger_dag_id="hello",
                                              param_list=params_list,
                                              default_args=DAG_ARGS)

        operator.execute(None)

        calls = session.add.call_args_list

