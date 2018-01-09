from airflow.operators.chunkflow_plugin import ChunkFlowOperator
from airflow.operators.chunkflow_plugin import chunkflow_subdag_from_file

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
TASK_ID = 'test_run_task'
PARENT_DAG_ID = 'parent_dag'
IMAGE_ID = 'alpine'
IMAGE_VERSION = 'latest'
COMMAND = 'echo "json is: \n %s"'


class TestChunkFlowOperator(object):
    def test_create(self):
        operator = ChunkFlowOperator(task_id=TASK_ID,
                                     default_args=DAG_ARGS)
        assert operator
        assert operator.task_id == TASK_ID
        assert operator.image == "%s:%s" % (ChunkFlowOperator.DEFAULT_IMAGE_ID,
                                            ChunkFlowOperator.DEFAULT_VERSION)

    def test_run_single(self, datadir):
        operator = ChunkFlowOperator(task_id=TASK_ID,
                                     default_args=DAG_ARGS,
                                     command=COMMAND,
                                     task_json='{"test":1}',
                                     image_id=IMAGE_ID,
                                     image_version=IMAGE_VERSION
                                     )
        operator.execute(None)


class TestChunkFlowTasksFileOperator(object):
    @staticmethod
    def create_parent_dag(parent_dag_id):
        return DAG(dag_id=parent_dag_id,
                   default_args=DAG_ARGS,
                   schedule_interval=None)

    @staticmethod
    def create_task(filename):
        parent_dag = TestChunkFlowTasksFileOperator.create_parent_dag(
            PARENT_DAG_ID)

        operator = chunkflow_subdag_from_file(filename,
                                              task_id=TASK_ID,
                                              image_id=IMAGE_ID,
                                              image_version=IMAGE_VERSION,
                                              default_args=DAG_ARGS,
                                              dag=parent_dag)
        return operator

    def test_empty(self, datadir):
        task_filename = datadir / 'empty.txt'
        operator = \
            TestChunkFlowTasksFileOperator.create_task(task_filename)

        assert operator
        assert operator.task_id == TASK_ID
        assert len(operator.subdag.task_ids) == 0

    def test_none(self, datadir):
        task_filename = datadir / 'no_tasks.txt'
        operator = \
            TestChunkFlowTasksFileOperator.create_task(task_filename)

        assert operator
        assert operator.task_id == TASK_ID
        assert len(operator.subdag.task_ids) == 0

    def test_single(self, datadir):
        task_filename = datadir / 'single.txt'
        operator = \
            TestChunkFlowTasksFileOperator.create_task(task_filename)

        assert operator
        assert operator.task_id == TASK_ID
        assert len(operator.subdag.task_ids) == 1
        assert operator.subdag.tasks[0].image == "%s:%s" % (
            IMAGE_ID,
            IMAGE_VERSION
            )

    def test_many(self, datadir):
        task_filename = datadir / 'many.txt'
        print(task_filename)
        operator = \
            TestChunkFlowTasksFileOperator.create_task(task_filename)

        assert operator
        assert operator.task_id == TASK_ID
        assert len(operator.subdag.tasks) == 8
        for task in operator.subdag.tasks:
            assert task.image == "%s:%s" % (IMAGE_ID,
                                            IMAGE_VERSION)
