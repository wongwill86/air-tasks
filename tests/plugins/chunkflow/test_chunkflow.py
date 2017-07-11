from airflow.operators.chunkflow_plugin import ChunkFlowOperator
from airflow.operators.chunkflow_plugin import ChunkFlowTasksFileOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
}


class TestChunkFlowOperator(object):
    def test_create(self):
        task_id = 'test_task'
        operator = ChunkFlowOperator(task_id, '{}', task_id='test_task')
        assert operator
        assert operator.task_id == task_id


class TestChunkFlowTasksFileOperator(object):
    @staticmethod
    def create_task_file(tmpdir, filename):
        tasks_file = tmpdir.join(filename)
        tasks_file.write('THIS')
        return tasks_file

    @staticmethod
    def create_parent_dag():
        return DAG(dag_id='parent',
                   default_args=default_args,
                   schedule_interval=None)

    def test_create(self, tmpdir):
        parent_dag_id = 'parent_dag'
        child_dag_id = 'child_dag'
        tasks_filename = 'testfile'

        parent_dag = TestChunkFlowTasksFileOperator.create_parent_dag()

        task_file = TestChunkFlowTasksFileOperator.create_task_file(
            tmpdir, tasks_filename)
        operator = ChunkFlowTasksFileOperator('testtask',
                                              str(task_file),
                                              dag=parent_dag)
        assert operator
        assert operator.task_id == '%s.%s' % (parent_dag_id, child_dag_id)
