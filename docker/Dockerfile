# VERSION 0.0.2
# AUTHOR: Will Wong
# DESCRIPTION: Docker airflow with ECR registry and DooD (Docker outside of Dkr)
# BUILD: docker build --rm -t wongwill86/air-tasks .
# SOURCE: https://github.com/wongwill86/air-tasks

ARG BASE_DIST=alpine
ARG BASE_TAG=0.0.2-customAF

FROM alpine/git as version_container
WORKDIR /.gitcopy
ADD .git .
RUN git rev-parse HEAD > COMMIT_VERSION
RUN git describe --tags > COMMIT_TAGS

FROM wongwill86/air-tasks:base-${BASE_DIST}-${BASE_TAG}
MAINTAINER wongwill86

ARG BASE_DIST
ARG BASE_TAG
ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ARG AIRFLOW_USER=airflow
ENV AIRFLOW_USER ${AIRFLOW_USER}

WORKDIR ${AIRFLOW_HOME}

# Move base version information directly into airflow home directory
RUN mkdir -p ${AIRFLOW_HOME}/version
RUN cp /COMMIT* ${AIRFLOW_HOME}/version

# prepare airflow user
RUN groupadd -r ${AIRFLOW_USER} \
    && useradd -r -d ${AIRFLOW_HOME} -g ${AIRFLOW_USER} -s /bin/bash ${AIRFLOW_USER} \
    && chown -R ${AIRFLOW_USER}: ${AIRFLOW_HOME} \
    && usermod -aG docker ${AIRFLOW_USER} \
    # unfortunately this is required to update the container docker gid to match the
    # host's gid, we remove this permission from entrypoint-dood.sh script
    && echo "${AIRFLOW_USER} ALL=NOPASSWD: ALL" >> /etc/sudoers

# Docker config (i.e. credentials )
RUN cp /.docker/ .docker -r

# Airflow celery config file into container
COPY config/celeryconfig.py config/celeryconfig.py

# Startup scripts
COPY scripts/ scripts

# Docker compose files
COPY deploy/docker-compose-CeleryExecutor.yml deploy/docker-compose-CeleryExecutor.yml

# Airflow configuration
COPY config/airflow.cfg airflow.cfg

# New Plugins
COPY plugins/ plugins

# New Dags
COPY dags/ dags/

# copy the current commit of this image from the version container
COPY --from=version_container \
    /.gitcopy/COMMIT_VERSION version/COMMIT_VERSION_AIR_TASKS
COPY --from=version_container \
    /.gitcopy/COMMIT_TAGS version/COMMIT_TAGS_AIR_TASKS.${BASE_DIST}-${BASE_TAG}

USER ${AIRFLOW_USER}
ENTRYPOINT ["scripts/entrypoint-dood.sh"]
