import pytest
from airflow.operators.chunkflow_plugin import ChunkFlowOperator
from airflow.operators.chunkflow_plugin import ChunkFlowTasksFileOperator


class TestChunkFlowPlugin(object):

    class TestChunkFlowOperator(object):
        def test_create(self):
            operator = ChunkFlowOperator('test_task', '{}', task_id='test_task')
            assert operator

    class TestChunkFlowTasksFileOperator(object):
        def test_create(self):
            parent_dag_id = 'parent_dag'
            child_dag_id = 'child_dag'
            tasks_filename = 'testfile'
            operator = ChunkFlowTasksFileOperator(parent_dag_id, child_dag_id,
                                                  tasks_filename)
            assert operator
