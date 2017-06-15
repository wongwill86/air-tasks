# VERSION 0.0.1
# AUTHOR: Will Wong
# DESCRIPTION: Docker airflow with ECR registry and DooD (Docker outside of Dkr)
# BUILD: docker build --rm -t wongwill86/air-tasks .
# SOURCE: https://github.com/wongwill86/air-tasks

# Compile AWS credential helper
FROM golang:1.8.3
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
RUN curl -fsSL https://get.docker.com/ | sh
RUN pip install docker-py
RUN apt-get install sudo

RUN apt-get clean \
    && rm -rf \
        /var/lib/apt/lists/* \
        /tmp/* \
        /var/tmp/* \
        /usr/share/man \
        /usr/share/doc \
        /usr/share/doc-base

RUN usermod -aG docker airflow

# unfortunately this is required to update the container docker gid to match the
# host's gid, we remove this permission from entrypoint-dood.sh script
RUN echo "airflow ALL=NOPASSWD: ALL" >> /etc/sudoers
WORKDIR ${AIRFLOW_HOME}/.docker

# this is to enable aws ecr credentials helpers to reauthorize docker
RUN echo '{\n    "credsStore": "ecr-login"\n}' > config.json
# copy the built docker credentials module to this container
COPY --from=0 \
    /go/src/github.com/awslabs/amazon-ecr-credential-helper/bin/local/docker-credential-ecr-login \
    /usr/local/bin

COPY deploy/scripts/entrypoint-dood.sh /entrypoint-dood.sh
COPY deploy/config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg

RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/entrypoint-dood.sh"]
