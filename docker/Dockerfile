# VERSION 0.0.3
# AUTHOR: Will Wong
# DESCRIPTION: Docker airflow with ECR registry and DooD (Docker outside of Dkr)
# BUILD: docker build --rm -t wongwill86/air-tasks .
# SOURCE: https://github.com/wongwill86/air-tasks

ARG BASE_DIST=alpine
ARG BASE_TAG=0.0.3-customAF
ARG BASE_TEST=

FROM wongwill86/air-tasks:base-${BASE_DIST}-${BASE_TAG}${BASE_TEST}
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

# Docker config (i.e. credentials helper from base image)
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

# Get commit hash and tags
WORKDIR ${AIRFLOW_HOME}/.git
COPY .git/refs/heads/ ./heads
COPY .git/refs/tags/ ./tags
COPY .git/HEAD ./HEAD
RUN cat $(cat HEAD | sed -e's/ref: refs\///g') > COMMIT \
    && grep $(cat COMMIT) tags/* -l | xargs -n 1 -r basename > TAGS \
    && cp COMMIT ${AIRFLOW_HOME}/version/COMMIT.air-tasks.${BASE_DIST}-${BASE_TAG} \
    && cp TAGS ${AIRFLOW_HOME}/version/TAGS.air-tasks.${BASE_DIST}-${BASE_TAG}

USER ${AIRFLOW_USER}
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["scripts/entrypoint-dood.sh"]
