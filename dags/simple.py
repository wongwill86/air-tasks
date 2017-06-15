import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from airflow.operators.docker_operator import DockerOperator
from fileflow.operators import DivePythonOperator
from fileflow.task_runners import TaskRunner

from airflow.operators import PythonOperator
from fileflow.operators.dive_operator import DiveOperator

class TaskRunnerExample(TaskRunner):
    def run(self, *args, **kwargs):
        output_string = "This task -- called {} -- was run.".format(self.task_instance.task_id)
        self.write_file(output_string)
        logging.info("Wrote '{}' to '{}'".format(output_string, self.get_output_filename()))

class TaskWriter(TaskRunner):
    def run(self, *args, **kwargs):
        logging.info("I am calling task writer!, writing to {}".format(
            self.get_output_filename()))
        self.write_file("Text written from task writer")

class TaskReader(TaskRunner):
    def run(self, *args, **kwargs):
        logging.info("I am calling task reader! Read {} from {}".format(
            self.read_upstream_file("thingy"),
            self.get_input_filename("thingy")))


class EnDivePythonOperator(DiveOperator, PythonOperator):
    """
    Python operator that can send along data dependencies to its callable.
    Generates the callable by initializing its python object and calling its method.
    """

    def __init__(self, python_object, python_method="run", *args, **kwargs):
        self.python_object = python_object
        self.python_method = python_method
        kwargs['python_callable'] = lambda: None 

        super(EnDivePythonOperator, self).__init__(*args, **kwargs)

    def pre_execute(self, context):
        context.update(self.op_kwargs)
        context.update({"data_dependencies": self.data_dependencies})
        instantiated_object = self.python_object(context)
        self.python_callable = getattr(instantiated_object, self.python_method)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 5, 1),
    'cactchup_by_default': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    }
dag = DAG("simple_ws", default_args=default_args, schedule_interval=None)


t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t2 = DockerOperator(
    task_id='watershed_sleep',
    image='watershed',
    command='/bin/sleep 10',
    network_mode='bridge',
    dag=dag)

t3 = BashOperator(
    task_id='print_hello',
    bash_command='echo "hello world!"',
    dag=dag)

t4 = BashOperator(
    task_id='print_hello_2',
    bash_command='echo "hello world 2!"',
    dag=dag)

t5 = BashOperator(
    task_id='print_goodbye',
    bash_command='echo "goodbye world!"',
    dag=dag)

t6 = EnDivePythonOperator(
    task_id="dive_python_writer",
    python_object=TaskWriter,
    provide_context=True,
    dag=dag)

t7 = EnDivePythonOperator(
    task_id="dive_python_reader",
    python_object=TaskReader,
    data_dependencies={"thingy": t6.task_id},
    provide_context=True,
    dag=dag)

tt = EnDivePythonOperator(
    task_id="write_a_file",
    python_method="run",
    python_object=TaskRunnerExample,
    provide_context=True,
    owner="airflow",
    dag=dag
)

t1.set_downstream(t2)
t2.set_downstream(t3)
t2.set_downstream(t4)
t3.set_downstream(t5)
t4.set_downstream(t5)
t5.set_downstream(t6)
t6.set_downstream(t7)
t7.set_downstream(tt)
