from __future__ import unicode_literals
from airflow.operators.custom_plugin import DockerWithVariablesOperator
import unittest
from tests.utils.mock_helpers import patch_plugin_file
# from airflow.models import Variable
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
TASK_ID = 'test_docker_with_variables'
IMAGE = 'alpine:latest'
MOUNT_POINT = '/run/variables'
COMMAND = 'ls %s' % MOUNT_POINT
COMMAND_CHECK_MOUNT = 'sh -c "mount | grep %s > /dev/null && ls %s"' \
    % (MOUNT_POINT, MOUNT_POINT)
COMMAND_SHOW_ITEMS = 'sh -c \
    "for file in $(ls %s); do echo $file; cat %s/$file; done"' \
    % (MOUNT_POINT, MOUNT_POINT)


DEFAULT_VARIABLES = {
    'char': 'a',
    'string': 'abcd',
    'key with spaces': '12345',
    'multiline': '''
                multiline
                string''',
    'weird': '\r\r\r\n%#@*(&(\x41\x42\x43'
}


class TestDockerWithVariables(unittest.TestCase):
    def test_create(self):
        operator = DockerWithVariablesOperator([],
                                               image=IMAGE,
                                               task_id=TASK_ID,
                                               default_args=DAG_ARGS)
        assert operator
        assert operator.task_id == TASK_ID
        operator.execute(None)

    def test_should_mount_and_be_empty(self):
        operator = DockerWithVariablesOperator(
            variables=[],
            mount_point=MOUNT_POINT,
            task_id=TASK_ID,
            default_args=DAG_ARGS,
            image=IMAGE,
            xcom_push=True,
            xcom_all=True,
            command=COMMAND_CHECK_MOUNT
            )
        items = operator.execute(None)  # will fail if no mount found
        assert not items  # mount was found but check to make sure it's empty

    @patch_plugin_file('plugins/custom/custom', 'Variable', autospec=True)
    def test_should_find_variables(self, variable_class):
        variable_class.get.side_effect = DEFAULT_VARIABLES.__getitem__

        operator = DockerWithVariablesOperator(
            variables=DEFAULT_VARIABLES.keys(),
            mount_point=MOUNT_POINT,
            task_id=TASK_ID,
            default_args=DAG_ARGS,
            image=IMAGE,
            xcom_push=True,
            xcom_all=True,
            command='ls /run/variables'
        )

        show_items = operator.execute(None)

        correct_show_items_builder = []
        for key in sorted(DEFAULT_VARIABLES):
            value = DEFAULT_VARIABLES[key]
            correct_show_items_builder.append(key)
            correct_show_items_builder.append(str(value))

        print('show_items\n\n\n')
        print(show_items)
        print('correct_show_items\n\n\n')
        print(correct_show_items_builder)
        print('correct_show_items joined \n\n\n')
        print('\n'.join(correct_show_items_builder))
        assert show_items == '\n'.join(correct_show_items_builder)

    @patch_plugin_file('plugins/custom/custom', 'Variable', autospec=True)
    def test_should_fail_when_variable_not_found(self, variable_class):
        variable_class.get.side_effect = DEFAULT_VARIABLES.__getitem__

        bad_keys = DEFAULT_VARIABLES.keys()
        bad_keys.append('bad_key')

        operator = DockerWithVariablesOperator(
            variables=bad_keys,
            mount_point=MOUNT_POINT,
            task_id=TASK_ID,
            default_args=DAG_ARGS,
            image=IMAGE,
            xcom_push=True,
            xcom_all=True,
            command='ls'
        )

        with self.assertRaises(KeyError):
            operator.execute(None)
