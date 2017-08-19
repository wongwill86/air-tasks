from __future__ import unicode_literals
from airflow.operators.custom_plugin import MultiTriggerDagRunOperator
from airflow import settings
from airflow.models import DagBag

from tests.utils.mock_helpers import patch_plugin_file

try:
    import unittest.mock as mock
except ImportError:
    import mock

import airflow
from airflow.models import DAG, DagBag
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


    @patch_plugin_file('multi_trigger_dag', 'DagBag', autospec=True)
    def test_execute(self, af):
        operator = MultiTriggerDagRunOperator(task_id=TASK_ID,
                                              trigger_dag_id="hello",
                                              param_list=["a"],
                                              default_args=DAG_ARGS)

        operator.execute(None)
        assert 1 == 0
