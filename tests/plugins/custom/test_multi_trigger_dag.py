from __future__ import unicode_literals
from airflow.operators.custom_plugin import MultiTriggerDagRunOperator

try:
    import unittest.mock as mock
except ImportError:
    import mock

# from airflow.models import DAG
from datetime import datetime, timedelta
# from pytest import fixture

# from distutils import dir_util
# import os

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
    def test_create(self):
        operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                              trigger_dag_id="hello",
                                              param_list=[],
                                              default_args=DAG_ARGS)
        assert operator

    # @mock.patch('airflow.settings.Session')
    def test_execute(self, settings_session):
        # settings_session = mock.MagicMock()
        # settings_session.return_value = {}
        print("here")

        operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                              trigger_dag_id="hello",
                                              param_list=["a"],
                                              default_args=DAG_ARGS)

        operator.execute(None)
        assert operator
        assert operator.task_id == 1
