from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.file import TemporaryDirectory


class DockerRemovableContainer(DockerOperator):
    def __init__(self,
                 remove=True,
                 *args, **kwargs):
        self.remove = remove
        super(DockerRemovableContainer, self).__init__(*args, **kwargs)

    def execute(self, context):
        try:
            return super(DockerRemovableContainer, self).execute(context)
        finally:
            if self.cli and self.container and self.remove:
                self.cli.stop(self.container, timeout=1)
                self.cli.remove_container(self.container)


class DockerWithVariablesOperator(DockerRemovableContainer):
    DEFAULT_MOUNT_POINT = '/run/variables'

    def __init__(self,
                 variables,
                 mount_point=DEFAULT_MOUNT_POINT,
                 *args, **kwargs):
        self.variables = variables
        self.mount_point = mount_point
        super(DockerWithVariablesOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        with TemporaryDirectory(prefix='dockervariables') as tmp_var_dir:
            for key in self.variables:
                value = Variable.get(key)
                with open('{0}/{1}'.format(tmp_var_dir, key), 'w') as \
                        value_file:
                    value_file.write(str(value))
            self.volumes.append('{0}:{1}'.format(tmp_var_dir,
                                                 self.mount_point))
            return super(DockerWithVariablesOperator, self).execute(context)


class CustomPlugin(AirflowPlugin):
    name = "docker_plugin"
    operators = [DockerRemovableContainer, DockerWithVariablesOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
