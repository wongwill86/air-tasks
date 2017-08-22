from __future__ import unicode_literals
import unittest
from airflow.operators.custom_plugin import MultiTriggerDagRunOperator
from airflow.utils.state import State
from airflow import settings

from tests.utils.mock_helpers import patch_plugin_file

try:
    import unittest.mock as mock
except ImportError:
    import mock

from datetime import datetime, timedelta

TRIGGER_DAG_ID = 'test_trigger_dag_id'
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


class TestMultiTriggerDag(unittest.TestCase):
    class DagRunWithParams(object):
        def __init__(self, parameters):
            self.parameters = parameters

        def __eq__(self, other):
            return (other['state'] == State.RUNNING and
                    other['external_trigger'] and
                    ((type(self.parameters) is dict and
                     self.parameters.viewitems() <= other['conf']) or
                    (self.parameters == other['conf'])))

        def __str__(self):
            return "Dag Run with parameters \"%s\"" % self.parameters

        def __repr__(self):
            return self.__str__()

    @staticmethod
    def create_mock_dag_bag():
        mock_dag = mock.MagicMock(name='Dag')
        mock_dag.create_dagrun.side_effect = lambda *args, **kwargs: kwargs

        test_dags = {}
        test_dags[TRIGGER_DAG_ID] = mock_dag

        mock_dag_bag = mock.MagicMock(name='DagBag')
        mock_dag_bag.get_dag.side_effect = lambda dag_id: test_dags.get(dag_id)

        return mock_dag_bag

    @staticmethod
    def verify_session(session, params_list):
        if params_list:
            for params in params_list:
                session.add.assert_any_call(
                    TestMultiTriggerDag.DagRunWithParams(params))
            assert session.add.call_count == len(params_list)
        else:
            assert session.add.call_count == 0

        session.commit.assert_called()

    @patch_plugin_file('plugins/custom/multi_trigger_dag', 'DagBag',
                       autospec=True)
    def test_execute_none_should_fail(self, dag_bag_class):
        params_list = None

        dag_bag_class.return_value = TestMultiTriggerDag.create_mock_dag_bag()

        session = settings.Session()
        session.add = mock.MagicMock(name='add')
        session.commit = mock.MagicMock(name='commit')

        with self.assertRaises(Exception):
            operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                                  trigger_dag_id=TRIGGER_DAG_ID,
                                                  param_list=params_list,
                                                  default_args=DAG_ARGS)
            operator.execute(None)

        session.add.assert_not_called()
        session.commit.assert_not_called()

    @patch_plugin_file('plugins/custom/multi_trigger_dag', 'DagBag',
                       autospec=True)
    def test_execute_empty(self, dag_bag_class):
        params_list = []

        dag_bag_class.return_value = TestMultiTriggerDag.create_mock_dag_bag()

        session = settings.Session()
        session.add = mock.MagicMock(name='add')
        session.commit = mock.MagicMock(name='commit')

        with self.assertRaises(Exception):
            operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                                  trigger_dag_id=TRIGGER_DAG_ID,
                                                  param_list=params_list,
                                                  default_args=DAG_ARGS)
            operator.execute(None)

        session.add.assert_not_called()
        session.commit.assert_not_called()

    @patch_plugin_file('plugins/custom/multi_trigger_dag', 'DagBag',
                       autospec=True)
    def test_execute_many(self, dag_bag_class):
        a = "a"
        b = "b"
        c = "c"
        d = "d"
        params_list = [a, b, c, d]

        dag_bag_class.return_value = TestMultiTriggerDag.create_mock_dag_bag()

        session = settings.Session()
        session.add = mock.MagicMock(name='add')
        session.commit = mock.MagicMock(name='commit')

        operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                              trigger_dag_id=TRIGGER_DAG_ID,
                                              param_list=params_list,
                                              default_args=DAG_ARGS)
        operator.execute(None)

        TestMultiTriggerDag.verify_session(session, params_list)

    @patch_plugin_file('plugins/custom/multi_trigger_dag', 'DagBag',
                       autospec=True)
    def test_execute_many_none(self, dag_bag_class):
        a = None
        b = None
        c = None
        d = None
        params_list = [a, b, c, d]

        dag_bag_class.return_value = TestMultiTriggerDag.create_mock_dag_bag()

        session = settings.Session()
        session.add = mock.MagicMock(name='add')
        session.commit = mock.MagicMock(name='commit')

        operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                              trigger_dag_id=TRIGGER_DAG_ID,
                                              param_list=params_list,
                                              default_args=DAG_ARGS)
        operator.execute(None)

        TestMultiTriggerDag.verify_session(session, params_list)
