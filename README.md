# Air-Tasks

A curated set of tools for managing distributed task workflows.

## Tools leveraged
* [Airflow](https://github.com/apache/incubator-airflow): Task Workflow Engine
* [Docker Swarm](https://docs.docker.com/engine/swarm/): Container Orchestration
* [Docker Infrakit](https://github.com/docker/infrakit): Infrastructure Orchestration to deploy on the cloud


Note: This project was inspired by https://github.com/puckel/docker-airflow and infrakit examples https://github.com/infrakit/examples

## Table of Contents
- [Core Concepts](#core-concepts)
- [Architectural Concepts](#architectural-concepts)
- [Setup](#setup)
- [Where to Start](#where-to-start)
  - [How to Run](#how-to-run)
  - [How to Develop Custom Dags](#how-to-develop-custom-dags)
  - [How to Package for Deployment](#how-to-package-for-deployment)
  - [How to Test](#how-to-test)
  - [How to Deploy](#how-to-deploy)
  - [Debug Tools](#debug-tools)
- [Notes](#notes)
  - [Nvidia GPU Docker Support](#nvidia-gpu-docker-support)
  - [Mounting Secrets](#mounting-secrets)
  - [Multiple Instance Types](#multiple-instance-types)
  - [Developing Plugins](#developing-plugins)
  - [AWS ECR Access](#aws-ecr-access)
  - [Base Images](#base-images)
- [Docker Cloud](#docker-cloud)

## Core Concepts:

### Tasks
Tasks are defined as independent and stateless units of work. A task is described by creating an [Airflow Operator](https://airflow.apache.org/concepts.html#operators)
Airflow provides many operators function such as calling bash scripts, python scripts, and even calling Docker images.

There is no guarantee that related tasks will be run on the same machine/environment. It is preferrable to use docker containers for your tasks to isolate the runtime environment and prevent polluting the environment of other tasks.

When a task is being scheduled to run, a `task_instance` is created.

### DAG
A Directed Acyclic Graph (DAG) is a static set of repeatable tasks operators that are invoked automatically or manually. DAGs are described in [Airflow DAGS]( https://airflow.apache.org/concepts.html#dags ). The nodes of this graph are the task operators and the edges describe the dependencies between them. Edges are created by setting `operator.set_upstream` or `operator.set_downstream` to and from each task operator.

It should be assumed that the size of a dag is immutable ( actually its not but it gets really messy if you modify it ). DAGS themselves can also be invoked using parameters.

See [example_trigger_target_dag](https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/example_trigger_target_dag.py)

When a DAG is being scheduled to run, a `dag_run` is created.

DAGs can be triggered by using the web ui as well as from the bash terminal of any airflow container, i.e.
```
$ docker exec -it <air_flow container> bash
$ airflow trigger_dag dag_id --conf '{"param_name":"param_value" }'
```

See more examples from https://github.com/wongwill86/air-tasks/tree/master/dags/examples

#### Useful (maybe?) Patterns
##### Standard
Create a one shot dag that is only run when manually triggered:

See https://github.com/wongwill86/air-tasks/blob/master/dags/examples/interleaved.py

This should be the most common use case. Should fit most needs.

##### Unbounded
Two separate DAGS are created:
1. Listener DAG: Listens for command to be triggered with parameters
2. Trigger DAG: Dynamically create a list of parameters to trigger the Listener DAG

See https://github.com/wongwill86/air-tasks/blob/master/dags/examples/multi_trigger.py

This should be avoided if possible since there is no good way to set fan-in dependencies for the listener DAG (possible but probably very hacky)

### Variables
These are global variables that all task operators can have access to.

See [Variables]( https://airflow.apache.org/concepts.html#variables )

### Compose File
This file is a schedule of services necessary to start Air-tasks

See https://github.com/wongwill86/air-tasks/blob/master/deploy/docker-compose-CeleryExecutor.yml

This is a description of all the services for [docker-compose]( https://docs.docker.com/compose/compose-file/ ).

#### Core Components
* **Postgres:** Database for saving DAGs, DAG runs, Tasks, Task Instances, etc...
* **RabbitMQ:** Internal queue service used to schedule tasks instances. Task instances are *only* scheduled when they are ready to run
* **Webserver:** Parses DAG python files and inserts them into the database
* **Scheduler:** Searches database for task_instances ready to run and places them in the queue
* **Flower:** Web UI monitoring of worker state and statistics
* **Worker (worker-worker):** Runs the task_instance
#### Additional Components
* **Worker (worker-manager):** Runs exclusively on Manager type instances. Runs tasks such as Autoscaling
* **Visualizer:** Basic Docker Swarm container visualizing UI
* **Proxy:** Reverse proxy for all web UI. Can be configured for basic auth and HTTPS
* **add-secrets:** Injects any specified secrets as a docker variable

## Architectural Concepts:

Deployment of Air-Tasks can be split into 3 layers of abstraction:

### Infrastructure Orchestration

[Docker Infrakit](https://github.com/docker/infrakit) to manage cloud services. Infrakit managers are able to start/stop and monitor cloud instances. Each manager/worker instance settings is defined from [manager.json](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/google/manager.json)/[worker.json](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/google/worker.json) for Google Cloud and [manager.json](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/aws/manager.json)/[worker.json](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/gcs/worker.json) for AWS. One manager node bootstraps the other nodes.

### Container Orchestration

[Docker Swarm](https://docs.docker.com/engine/swarm/) to join separate machines into a cluster. Docker managers are able to deploy and monitoring services. Services are defined in the [compose file](#compose-file). Manager nodes run all services except for `worker-worker`. This is made possible via deploy constraints using the engine label `infrakit-manager=true`.

### Task Orchestration
[Airflow](https://github.com/apache/incubator-airflow) tasks are run on worker-worker containers that, in turn, run on infrakit worker nodes.

#### How a task is executed:
1. Webserver parses python DAG file and inserts into database
2. DAG is triggered either manually via web/cli or via cron schedule
3. Scheduler creates `dag_runs` and `task_instances` for DAG
4. Scheduler inserts any valid ready `task_instances` into the queue
5. Worker processes tasks
6. Worker writes back to queue and database indicating status as done

### Autoscaling
Air-Tasks is capable of autoscaling the cluster by monitoring the number of tasks ready to be executed (on the queue). This is done by careful coordination between the above 3 layers.

A special worker service ("worker-manager") is created in the [compose file](#compose-file). This service is deployed exclusively on manager nodes, thus capable of creating instances via infrakit. Additionally a separate queue topic ("worker-manager") is dedicated for tasks that need to run on managers.

See https://github.com/wongwill86/air-tasks/blob/master/dags/manager/scaler.py for more information

## Setup
1. Install docker
    * **NO** Nvidia GPU support needed
        ```
        wget -qO- https://get.docker.com/ | sh
        ```
    * **YES** Nvidia GPU support needed
        1. [Install Docker CE](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#install-using-the-repository)
        2. Install [nvidia-docker2](https://github.com/NVIDIA/nvidia-docker#xenial-x86_64)

2. Install docker compose
    ```
    pip install docker-compose
    ```

## Where to Start
### How to Run
1. [Install requirements](#setup)
2. Clone this repo
3. [Deploy Local](#local)
4. Go to [localhost](http://localhost)
5. Activate dag and trigger run

### How to Develop Custom Dags
1. Uncomment **every** dag and plugin folder mounts in deploy/docker-compose-CeleryExecutor.yml
    ```
    #- ../dags/:/usr/local/airflow/dags
    #- ../plugins:/usr/local/airflow/plugins
    ```
2. Create or modify DAG inside [dags folder](https://github.com/wongwill86/air-tasks/tree/master/dags)
3. Check [webserver](http://localhost) to see if DAG is now updated

See other [examples](https://github.com/wongwill86/air-tasks/tree/master/dags/examples) for inspiration

### How to Package for Deployment
1. Build docker image
    ```
    docker build -f docker/Dockerfile -t wongwill86/air-tasks:<your tag> .
    ```
2. Before committing/pushing, comment **every** dag and plugin folder mounts in deploy/docker-compose-CeleryExecutor.yml
    ```
    #- ../dags/:/usr/local/airflow/dags
    #- ../plugins:/usr/local/airflow/plugins
    ```
3.  Replace **every** air-tasks tag with your tag in deploy/docker-compose-CeleryExecutor.yml
    ```
    <every service that has this>:
        image: wongwill86/air-tasks:<your tag>
    ```
4. Try to [Deploy](#local)
5. Push to docker and/or wait for docker cloud to build

### How to Deploy
#### Local
```
docker-compose -f deploy/docker-compose-CeleryExecutor.yml up -d
```

#### Swarm
```
echo '<blank or username here>' | docker secret create basic_auth_username -
echo '<blank or password here>' | docker secret create basic_auth_password -
echo '<blank ssl certificate here>' | docker secret create ssl_certificate -
echo '<blank ssl certificate key here>' | docker secret create ssl_certificate_key -
docker stack deploy -c deploy/docker-compose-CeleryExecutor.yml <stack name>
```

#### AWS
1. Initialize submodule
    ```
    git submodule init
    git submodule update --recursive --remote
    ```
2. Use [Cloudformation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new) to create a new stack.
3. Use this [cloud/latest/swarm/aws/vpc.cfn](https://raw.githubusercontent.com/wongwill86/examples/air-tasks/latest/swarm/aws/vpc.cfn)

#### GCloud
1. Initialize submodule
    ```
    git submodule init
    git submodule update --recursive --remote
    ```
2. Install [gcloud](https://cloud.google.com/sdk/downloads)
3. *(Optional)* configure yaml [cloud/latest/swarm/google/cloud-deployment.yaml](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/google/cloud-deployment.yaml)
4. Deploy using gcloud
    ```
    gcloud deployment-manager deployments create <deployment name> --config cloud/latest/swarm/google/cloud-deployment.yaml
    ```

### How to Test
1. [Install requirements](#setup)
2. Clone this repo
3. Build the test image
    ```
    export PYTHONDONTWRITEBYTECODE=1 
    export IMAGE_NAME=wongwill86/air-tasks:<your tag>
    docker-compose -f docker/docker-compose.test.yml -p ci build
    ```
4. Run test container
    ```
    docker-compose -f docker/docker-compose.test.yml -p ci run --rm sut
    ```
5. *(Optional)* Watch / Test. 
    ```
    docker-compose -f docker/docker-compose.test.yml -p ci run --rm sut ptw -- --pylama
    ```
    *Warning 1: if nothing runs, make sure all tests pass first*

    *Warning 2: you may need to restart if you rename/move files, especially possible if these are plugin modules*

## Debug Tools
[AirFlow](http://localhost) or (`<host>/`) - Airflow Webserver

[Celery Flower](http://localhost/flower) or (`<host>/flower`)- Monitor Workers

[Swarm Visualizer](http://localhost/visualizer) or (`<host>/visualizer`)- Visualize Stack Deployment

[RabbitMQ](http://localhost/rabbitmq) or (`<host>/rabbitmq`)- RabbitMQ Management Plugin (Queue Info)

Note: if running with ssl, use https: instead of http

## Notes
### Nvidia GPU Docker Support
#### Setup Notes
Nvidia-docker is not forward compatible with edge releases of Docker. Therefore you must install the latest *stable* version of docker. As of writing the latest nvidia-docker can only be used with docker version 17.09. Check to make sure that the version is correct by using `docker version`.  

Repeated from [Setup](#setup)
1. [Install Docker CE](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/#install-using-the-repository)
2. Install [nvidia-docker2](https://github.com/NVIDIA/nvidia-docker#xenial-x86_64)

nvidia-docker2 will add configurations via `/etc/docker/daemon.json`. If you have made any changes i.e. [multiple-instance-types](#multiple-instance-types), make sure those are still persisted.

#### Driver Compatibility
If you are using CUDA in your docker image, please make sure that the CUA version matches the host's Nvidia driver version.
See [compatibility matrix](https://github.com/NVIDIA/nvidia-docker/wiki/CUDA) for more details

#### Docker Operator with Nvidia

Use any air-task's [custom docker operators](https://github.com/wongwill86/air-tasks/blob/gpu/plugins/custom/docker_custom.py) with runtime set. i.e.

```
start = DockerConfigurableOperator(
    task_id='docker_gpu_task',
    command='nvidia-smi',
    default_args=default_args,
    image='nvidia/cuda:8.0-runtime-ubuntu16.04',
    host_args={'runtime': 'nvidia'},
    dag=dag
)

```
### Mounting Secrets

If your docker operator requires secrets, you can add them using [variables]( https://airflow.apache.org/concepts.html#variables ). Then you can mount these secrets using [DockerWithVariablesOperator](https://github.com/wongwill86/air-tasks/blob/master/dags/examples/docker_with_variables.py). i.e.

```
start = DockerWithVariablesOperator(
    ['your_key'],
    mount_point='/secrets',
    task_id='docker_task',
    command='sh -c "ls /secrets &&\
        cat /secrets/variables/your_key && echo done"',
    default_args=default_args,
    image='alpine:latest',
    dag=dag
)
```

#### Running/testing locally
You can mount your host machine's directory as a volume in the Docker Operator. DockerOperator calls docker directly from the host machine. In other words, your task's docker container is not running inside the worker container. Instead, it is running directly off the host machine. Therefore when you are testing locally, you can just mount your host's secret directory as a volume into the operator.

### Multiple Instance Types
**INCOMPLETE**
If you need to run tasks on different machine instance types, this can be achieved by scheduling the task on a new queue topic.  Currently all standard workers listen to the queue topic `worker`. If you require specialized workers to run specific tasks, this can be achieved by:

1. In the *task operator*, specify a new queue topic. This will schedule all tasks of this operator into a separate queue topic (in this case `other-instance-type`).
    ```
    start = BashOperator(
        task_id='new_instance_tag',
        bash_command='echo run from other instance type',
        queue='other-instance-type',
        dag=dag)
    ```
2. In the *[docker compose file](https://github.com/wongwill86/air-tasks/blob/master/deploy/docker-compose-CeleryExecutor.yml)*, create a new service copied and pasted from `worker-worker`. This will create workers that will listen to this new queue topic (`other-instance-type`) and only deploy on machines with the docker engine label: `[ engine.labels.infrakit-role == other-instance-type ]`.
    ```
    worker-other-instance-type:

        ...

        command: worker -q other-instance-type
        deploy:
            mode: global
            placement:
                constraints: [ engine.labels.infrakit-role == other-instance-type ]
    ```
3. Add support for new workers in Infrakit.
    1. Create a new worker init script to set the role to `worker-other-instance-type` i.e.  [cloud/latest/swarm/worker-init.sh](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/worker-init.sh). This role is used to set the docker engine label (to deploy your new docker service that listens to the queue topic `other-instance-type`).
    2. Create a new worker definition i.e. [cloud/latest/swarm/google/worker.json](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/google/worker.json). This is used to specify the instance type.
    3. Add a new group plugin with ID `worker-other-instance-type` to enable worker definitions created from Steps 1 and 2 [cloud/latest/swarm/groups.json](https://github.com/wongwill86/examples/blob/air-tasks/latest/swarm/groups.json). If you create an ID in this format, autoscaling will work for this instance type.

### Developing Plugins

Sometimes you may need to make operators that will be useful for others. These can be shared with others as a plugin. You can add plugins to the [plugins folder](https://github.com/wongwill86/air-tasks/tree/master/plugins).

See https://github.com/wongwill86/air-tasks/blob/master/plugins/custom/docker.py

### AWS ECR Access

To access a private AWS container registry, remember to set aws environment variables such as:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_DEFAULT_REGION

Docker login to AWS ECR will automatically be set up.

### Base Images
The main [Dockerfile](https://github.com/wongwill86/air-tasks/blob/master/docker/Dockerfile) is built on top of one of two base images:
* [Alpine](https://github.com/wongwill86/air-tasks/blob/master/docker/base/Dockerfile.base-alpine)(default)
* [Slim](https://github.com/wongwill86/air-tasks/blob/master/docker/base/Dockerfile.base-slim).

Additionally, this base image is used to build the test image which includes python test libraries. This is useful for testing derived images so that the test libraries do not need to be reinstalled. See [docker-compose.test.yml](https://github.com/wongwill86/air-tasks/blob/master/docker/docker-compose.test.yml) for more details. These base images should automatically be built in docker cloud.

If for any reason you require building new base images:
1. Build the base image
    ```
    docker build -f docker/base/Dockerfile.base-alpine -t wongwill86/air-tasks:<your base tag> .
    ```
2. Prepare base image to build test base image
    ```
    export IMAGE_NAME=wongwill86/air-tasks:<your base tag>
    ```
3. Build test base image
    ```
    docker-compose -f docker/docker-compose.test.yml -p ci_base build
    ```
4.  retag build test base image for testing
    ```
    docker tag ci_base_sut wongwill86/air-tasks:<your base tag>-test
    ```

## Docker Cloud

| Source Type | Source | Docker Tag | Dockerfile location | Build Context
| - | - | - | - | - |
| Branch | master | latest | Dockerfile | / |
| Branch | /^(.{1,5}&#124;.{7}&#124;([^m]&#124;m[^a]&#124;ma[^s]&#124;mas[^t]&#124;mast[^e]&#124;maste[^r])(.*))$/ | {source-ref | docker/Dockerfile | / |
| Tag | /^base-([0-9.a-zA-Z-]+)$/ | base-alpine-{\1} | docker/base/Dockerfile.base-alpine | / |
| Tag | /^base-([0-9.a-zA-Z-]+)$/ | base-slim-{\1} | docker/base/Dockerfile.base-slim | / |
