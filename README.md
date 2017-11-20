# Air-Tasks

A curated set of tools for managing distributed task workflows.

## Tools leveraged
* [Airflow](https://github.com/apache/incubator-airflow): Task Workflow Engine
* [Docker Swarm](https://docs.docker.com/engine/swarm/): Container Orchestration
* [Docker Infrakit](https://github.com/docker/infrakit): Infrastructure Orchestration to deploy on the cloud


Note: This project builds off of the docker image provided by https://github.com/puckel/docker-airflow and infrakit examples https://github.com/infrakit/examples

## Table of Contents
- [Core Concepts](#core-concepts)
- [Architectural Concepts](#architectural-concepts)
- [Setup](#setup)
- [How to Start](#how-to-start)
- [How to Deploy](#how-to-deploy)
- [Debug Tools](#debug-tools)
- [Notes](#notes)

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

See https://github.com/wongwill86/air-tasks/blob/master/docker/docker-compose-CeleryExecutor.yml

This is a description of all the services for [docker-compose]( https://docs.docker.com/compose/compose-file/ ).

#### Core Components
* **Postgres:** Database for saving DAGs, DAG runs, Tasks, Task Instances, etc...
* **RabbitMQ:** Internal queue service used to schedule tasks instances. Task instances are *only* scheduled when they are ready to run.
* **Webserver:** Parses DAG python files and inserts them into the database.
* **Scheduler:** Searches database for task_instances ready to run and places them in the queue.
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

[Docker Swarm](https://docs.docker.com/engine/swarm/) to join separate machines into a cluster. Docker managers are able to deploy and monitoring services. Services are defined in the [compose file](#compose-file). Manager nodes run all services except for worker-worker. This is done via deploy constraints using the engine label `infrakit-role=manager`.

### Task Orchestration
[Airflow](https://github.com/apache/incubator-airflow) tasks are run on worker-worker containers that in turn run on infrakit worker nodes.

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
	```
	wget -qO- https://get.docker.com/ | sh
	```
2. Install docker compose
    ```
    pip install docker-compose
    ```

## How to Start
1. [Install requirements](#setup)
2. Clone this repo
3. Uncomment **every** dag and plugin folder mounts in docker/docker-compose-CeleryExecutor.yml
	```
	#- ../dags/:/usr/local/airflow/dags
	#- ../plugins:/usr/local/airflow/plugins
	```
4. Create your DAG inside [dags folder](https://github.com/wongwill86/air-tasks/tree/master/dags)
5. (Optional) Start [Tests](#automated-testing)
6. [Deploy Local](#local)
7. Go to [localhost](http://localhost)
8. Activate dag and trigger run

See other [examples](https://github.com/wongwill86/air-tasks/tree/master/dags/examples) for inspiration

### Automated Testing
1. Do above steps 1-2
2. Build the image with your own tag (good idea to use the branch name)
    ```
    docker build -f docker/Dockerfile -t wongwill86/air-tasks:<your tag> .
    ```
3. Replace **every** air-tasks tag with your tag in docker/docker-compose-CeleryExecutor.yml
    ```
    <every service that has this>:
        image: wongwill86/air-tasks:<your tag>
    ```
4. Deploy [Tests](#testing)

## How to Deploy
### Local
```
docker-compose -f docker/docker-compose-CeleryExecutor.yml up -d
```

### Swarm
```
echo '<blank or username here>' | docker secret create basic_auth_username -
echo '<blank or password here>' | docker secret create basic_auth_password -
echo '<blank ssl certificate here>' | docker secret create ssl_certificate -
echo '<blank ssl certificate key here>' | docker secret create ssl_certificate_key -
docker stack deploy -c docker/docker-compose-CeleryExecutor.yml <stack name>
```

### Testing
```
export PYTHONDONTWRITEBYTECODE=1 
export IMAGE_NAME=wongwill86/air-tasks:<your branch/tag>
docker-compose -f docker/docker-compose.test.yml -p ci build
docker-compose -f docker/docker-compose.test.yml -p ci run --rm sut
```

To watch/test. (Warning: if nothing runs, make sure all tests pass first)
```
docker-compose -f docker/docker-compose.test.yml -p ci run --rm sut ptw -- --pylama
```

### AWS
1. Optional: Initialize submodule
	```
	git submodule update --recursive --remote
	```
2. Use [Cloudformation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new) to create a new stack.
3. Use this [cloud/latest/swarm/aws/vpc.cfn](https://raw.githubusercontent.com/wongwill86/examples/air-tasks/latest/swarm/aws/vpc.cfn)

### GCloud
1. Optional: Initialize submodule
	```
	git submodule update --recursive --remote
	```
2. Install [gcloud](https://cloud.google.com/sdk/downloads)
3. Optional: configure yaml (cloud/latest/swarm/google/cloud-deployment.yaml)
4. Deploy using gcloud
	```
	gcloud deployment-manager deployments create <deployment name> --config cloud/latest/swarm/google/cloud-deployment.yaml
	```

## Debug Tools
[AirFlow](http://localhost) - Airflow Webserver

[Celery Flower](http://localhost/flower) - Monitor Workers

[Swarm Visualizer](http://localhost/visualizer) - Visualize Stack Deployment

[RabbitMQ](http://localhost/rabbitmq) - RabbitMQ Management Plugin (Queue Info)

Note: if running with ssl, use https: instead of http

## Notes
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
    image="alpine:latest",
    dag=dag
)
```

### Developing Plugins

Sometimes you may need to make operators that will be useful for others. These can be shared with others as a plugin. You can add plugins to the [plugins folder](https://github.com/wongwill86/air-tasks/tree/master/plugins).

See https://github.com/wongwill86/air-tasks/blob/master/plugins/custom/docker.py

### AWS ECR Access

To access a private AWS container registry, remember to set aws environment variables such as:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_DEFAULT_REGION

Docker login to AWS ECR will automatically be set up.
