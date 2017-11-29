import os
import json
import logging
from docker import APIClient as Client, tls
from airflow.exceptions import AirflowException
from airflow.plugins_manager import AirflowPlugin
from airflow.models import Variable
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.file import TemporaryDirectory


class DockerConfigurableOperator(DockerOperator):
    """
    This is modified from https://github.com/apache/incubator-airflow/blob/1.8.2/airflow/operators/docker_operator.py
    with the exception that we are able to inject container and host arguments
    before the container is run.
    """ # noqa
    def __init__(self, container_args={}, host_args={}, *args, **kwargs):
        self.container_args = container_args
        self.host_args = host_args
        super(DockerConfigurableOperator, self).__init__(*args, **kwargs)

    # This needs to be updated whenever we update to a new version of airflow!
    def execute(self, context):
        logging.info('Starting docker container from image ' + self.image)

        tls_config = None
        if self.tls_ca_cert and self.tls_client_cert and self.tls_client_key:
            tls_config = tls.TLSConfig(
                    ca_cert=self.tls_ca_cert,
                    client_cert=(self.tls_client_cert, self.tls_client_key),
                    verify=True,
                    ssl_version=self.tls_ssl_version,
                    assert_hostname=self.tls_hostname
            )
            self.docker_url = self.docker_url.replace('tcp://', 'https://')

        self.cli = Client(base_url=self.docker_url, version=self.api_version,
                          tls=tls_config)

        if ':' not in self.image:
            image = self.image + ':latest'
        else:
            image = self.image

        if self.force_pull or len(self.cli.images(name=image)) == 0:
            logging.info('Pulling docker image ' + image)
            for l in self.cli.pull(image, stream=True):
                output = json.loads(l.decode('utf-8'))
                logging.info("{}".format(output['status']))

        cpu_shares = int(round(self.cpus * 1024))

        with TemporaryDirectory(prefix='airflowtmp') as host_tmp_dir:
            self.environment['AIRFLOW_TMP_DIR'] = self.tmp_dir
            self.volumes.append('{0}:{1}'.format(host_tmp_dir, self.tmp_dir))

            host_args = {
                'binds': self.volumes,
                'network_mode': self.network_mode
            }
            host_args.update(self.host_args)

            container_args = {
                'command': self.get_command(),
                'cpu_shares': cpu_shares,
                'environment': self.environment,
                'image': image,
                'mem_limit': self.mem_limit,
                'user': self.user,
                'host_config': self.cli.create_host_config(**host_args)
            }

            container_args.update(self.container_args)

            self.container = self.cli.create_container(**container_args)

            self.cli.start(self.container['Id'])

            line = ''
            for line in self.cli.logs(container=self.container['Id'],
                                      stream=True):
                logging.info("{}".format(line.strip()))

            exit_code = self.cli.wait(self.container['Id'])
            if exit_code != 0:
                raise AirflowException('docker container failed')

            if self.xcom_push:
                return self.cli.logs(
                    container=self.container['Id']) if self.xcom_all else str(
                        line.strip())


class DockerRemovableContainer(DockerConfigurableOperator):
    """
    This manually removes the container after it has exited.
    This is *NOT* to be confused with docker host config with *auto_remove*
    AutoRemove is done on docker side and will automatically remove the
    container along with its logs automatically before we can display the logs
    and get the exit code!
    """
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
                with open(os.path.join(tmp_var_dir, key), 'w') as value_file:
                    value_file.write(str(value))
            self.volumes.append('{0}:{1}'.format(tmp_var_dir,
                                                 self.mount_point))
            return super(DockerWithVariablesOperator, self).execute(context)


class CustomPlugin(AirflowPlugin):
    name = "docker_plugin"
    operators = [DockerRemovableContainer, DockerWithVariablesOperator,
                 DockerConfigurableOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
