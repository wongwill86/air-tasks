# VERSION 0.0.1
# AUTHOR: Will Wong
# DESCRIPTION: Docker airflow with ECR registry and DooD (Docker outside of Dkr)
# BUILD: docker build --rm -t wongwill86/air-tasks .
# SOURCE: https://github.com/wongwill86/air-tasks

# Compile AWS credential helper
FROM golang:1.8.3 as aws_ecr_credential_helper
WORKDIR /go/src/github.com/awslabs/
RUN git clone https://github.com/awslabs/amazon-ecr-credential-helper.git
WORKDIR /go/src/github.com/awslabs/amazon-ecr-credential-helper
RUN make

FROM puckel/docker-airflow:1.8.1
MAINTAINER wongwill86

ARG AIRFLOW_HOME=/usr/local/airflow
ENV AIRFLOW_HOME ${AIRFLOW_HOME}
ENV AIRFLOW_USER airflow

USER root
RUN curl -fsSL https://get.docker.com/ | sh \
    && pip install docker docker-compose \
    && pip install celery -U \
    && apt-get install sudo \
    && buildDeps=' \
        python-dev \
        libkrb5-dev \
        libsasl2-dev \
        libssl-dev \
        libffi-dev \
        build-essential \
        libblas-dev \
        liblapack-dev \
        libpq-dev \
        git \
    ' && apt-get remove --purge -yqq $buildDeps \
    && apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

# SUPER HACK PLEASE REMOVE AFTER AIRFLOW UPDATES (i.e. https://github.com/apache/incubator-airflow/pull/2417)
RUN sed -i -e 's/import Client/import APIClient as Client/' /usr/local/lib/python2.7/dist-packages/airflow/operators/docker_operator.py

RUN usermod -aG docker airflow

# unfortunately this is required to update the container docker gid to match the
# host's gid, we remove this permission from entrypoint-dood.sh script
RUN echo "airflow ALL=NOPASSWD: ALL" >> /etc/sudoers

# this is to enable aws ecr credentials helpers to reauthorize docker
RUN mkdir -p ${AIRFLOW_HOME}/.docker/ \
    && echo '{\n    "credsStore": "ecr-login"\n}' > \
        ${AIRFLOW_HOME}/.docker/config.json
RUN chown airflow ${AIRFLOW_HOME}/.docker/config.json

# copy the built docker credentials module to this container
COPY --from=aws_ecr_credential_helper \
    /go/src/github.com/awslabs/amazon-ecr-credential-helper/bin/local/docker-credential-ecr-login \
    /usr/local/bin

COPY docker/docker-compose-CeleryExecutor.yml ${AIRFLOW_HOME}/docker/docker-compose-CeleryExecutor.yml
RUN chown airflow ${AIRFLOW_HOME}/docker/docker-compose-CeleryExecutor.yml

COPY docker/scripts/add-user-docker.sh /add-user-docker.sh
COPY docker/scripts/entrypoint-dood.sh /entrypoint-dood.sh

COPY docker/config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
RUN chown airflow ${AIRFLOW_HOME}/airflow.cfg

ENV CELERY_CONFIG_MODULE celeryconfig
COPY docker/config/celeryconfig.py ${AIRFLOW_HOME}/celeryconfig.py
RUN chown airflow ${AIRFLOW_HOME}/celeryconfig.py

ENV CELERY_LOADER celery_loader:CeleryLoader
COPY docker/config/celery_loader.py ${AIRFLOW_HOME}/celery_loader.py
RUN chown airflow ${AIRFLOW_HOME}/celery_loader.py

COPY plugins/ ${AIRFLOW_HOME}/plugins/

COPY dags/ ${AIRFLOW_HOME}/dags/
RUN chown airflow ${AIRFLOW_HOME}/dags

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint-dood.sh"]
