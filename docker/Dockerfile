# VERSION 0.0.2
# AUTHOR: Will Wong
# DESCRIPTION: Docker airflow with ECR registry and DooD (Docker outside of Dkr)
# BUILD: docker build --rm -t wongwill86/air-tasks .
# SOURCE: https://github.com/wongwill86/air-tasks

FROM wongwill86/air-tasks:alpine-base
MAINTAINER wongwill86

ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ARG AIRFLOW_USER=airflow
ENV AIRFLOW_USER ${AIRFLOW_USER}

# prepare airflow user
RUN mkdir -p ${AIRFLOW_HOME} \
    && groupadd -r ${AIRFLOW_USER} \
    && useradd -r -d ${AIRFLOW_HOME} -g ${AIRFLOW_USER} -s /bin/bash ${AIRFLOW_USER} \
    && chown -R ${AIRFLOW_USER}: ${AIRFLOW_HOME} \
    && usermod -aG docker ${AIRFLOW_USER} \
    # unfortunately this is required to update the container docker gid to match the
    # host's gid, we remove this permission from entrypoint-dood.sh script
    && echo "${AIRFLOW_USER} ALL=NOPASSWD: ALL" >> /etc/sudoers

# Docker config (i.e. credentials )
RUN cp /.docker/ ${AIRFLOW_HOME}/.docker -r

# Airflow celery config file into container
COPY docker/config/celeryconfig.py ${AIRFLOW_HOME}/config/celeryconfig.py

# Startup scripts
COPY docker/scripts/ /

# Docker compose files
COPY docker/docker-compose-CeleryExecutor.yml ${AIRFLOW_HOME}/docker/docker-compose-CeleryExecutor.yml

# Airflow configuration
COPY docker/config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

# New Plugins
COPY plugins/ ${AIRFLOW_HOME}/plugins

# New Dags
COPY dags/ ${AIRFLOW_HOME}/dags/

USER ${AIRFLOW_USER}
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint-dood.sh"]
