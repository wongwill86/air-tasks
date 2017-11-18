# Air-Tasks, a Distributed Task Workflow Management System

Curated set of tools to enable distributed task processing.

Tools leveraged:
* [Airflow](https://github.com/apache/incubator-airflow): Task Workflow Engine
* [Docker Swarm](https://docs.docker.com/engine/swarm/): Container Orchestration
* [Docker Infrakit](https://github.com/docker/infrakit): Infrastructure Orchestration to deploy on the cloud


Note: This project builds off of the docker image provided by https://github.com/puckel/docker-airflow and infrakit examples https://github.com/infrakit/examples

## Concepts:

### Tasks
Tasks are defined as independent and stateless units of work. A task is described by creating an [Airflow Operator](https://airflow.apache.org/concepts.html#operators)
Airflow provides many operators function such as calling bash scripts, python scripts, and even calling Docker images.

There is no guarantee that related tasks will be run on the same machine/environment. It is preferrable to use docker containers for your tasks to isolate the runtime environment and prevent polluting the environment of other tasks.

When a task is being scheduled to run, a `task_instance` is created.

### DAG
A Directed Acyclic Graph (DAG) is a static set of repeatable tasks operators that are invoked automatically or manually. DAGs are described in [ Airflow DAGS ]( https://airflow.apache.org/concepts.html#dags ). The nodes of this graph are the task operators and the edges describe the dependencies between them. Edges are created by setting `task.set_upstream` or `task.set_downstream` to and from each task operator.

It should be assumed that the size of a dag is immutable ( actually its not but it gets really messy if you modify it ). DAGS themselves can also be invoked using parameters. See [ example_trigger_target_dag ](https://github.com/apache/incubator-airflow/blob/master/airflow/example_dags/example_trigger_target_dag.py).

When a DAG is being scheduled to run, a `dag_run` is created.

DAGs can be triggered by using the web ui as well as from thhe bash terminal of any airflow container, i.e.
```
$ docker exec -it <air_flow container> bash
$ airflow trigger_dag dag_id --conf '{"param_name":"param_value" }'
```

See more examples from https://github.com/wongwill86/air-tasks/tree/master/dags/examples

See examples from https://github.com/wongwill86/air-tasks/tree/master/dags/examples

#### Useful (maybe?) Patterns
##### Standard
Create a one shot dag that is only run when manually triggered:
see https://github.com/wongwill86/air-tasks/blob/master/dags/examples/interleaved.py

This should be the most common use case. Should fit most needs.

##### Unbounded
Two separate DAGS are created:
1. Listener DAG. Listens for command to be triggered with parameters
2. Trigger DAG. Dynamically create a list of parameters to trigger the Listener DAG
see https://github.com/wongwill86/air-tasks/blob/master/dags/examples/multi_trigger.py

This should be avoided if possible since there is no good way to set fan-in dependencies for the listener DAG (possible but probably very hacky)

### Variables
These are global variables that can be set to be made available for all tasks. see [ Variables ]( https://airflow.apache.org/concepts.html#variables )

### Compose File
This file is a schedule of services necessary to start Air-tasks

See https://github.com/wongwill86/air-tasks/blob/master/docker/docker-compose-CeleryExecutor.yml

This is a description of all the services for [ docker-compose ]( https://docs.docker.com/compose/compose-file/ ). This file includes all the services required to start up your containers.

#### Core Airflow Components
##### Postgres
Database for saving DAGs, DAG runs, Tasks, Task Instances, etc...
##### RabbitMQ
Internal queue service used to schedule tasks instances. Task instances are *only* scheduled when they are ready to run.
##### Webserver
Parses DAG python files and inserts them into the database.
##### Scheduler
Searches database for task_instances ready to run and places them in the queue.
##### Flower
Web UI monitoring of worker state and statistics
##### Worker (worker-worker)
Runs the task_instance
#### Additional Components
##### worker-manager
Runs exclusively on Manager type instances. Runs tasks such as Autoscaling
##### Visualizer
Basic Docker Swarm container visualizing UI
##### Proxy
Reverse proxy for all web UI. Can be configured for basic auth and HTTPS
#### add-secrets:
Injects any specified secrets as a docker variable


#### Architectural Concepts:
####Infrastructure Orchestration
[Docker Infrakit](https://github.com/docker/infrakit) to deploy into the cloud. Each manager and worker instance is defined 
####Container Orchestration
[Docker Swarm](https://docs.docker.com/engine/swarm/) 
####Task Orchestration
[Airflow](https://github.com/apache/incubator-airflow)

## How to develop:

### By running DAGS:
1. Clone this repo
2. [Install requirements](#setup)
3. Modify docker/docker-compose-CeleryExecutor.yml and uncomment dag folder mounts
	```
	#- ../dags/:/usr/local/airflow/dags
	```
5. [Deploy Local](#local)
6. Go to [localhost](http://localhost)
7. Activate dag and trigger run

### By testing plugins / new operators / DockerFile / DAGS:
1. Do above steps 1-3
2. Build the image with your own tag (good idea to use the branch name)
    ```
    docker build -f docker/Dockerfile -t wongwill86/air-tasks:<your tag> .
    ```
3. Modify docker/docker-compose-CeleryExecutor.yml to use your image
    ```
    <every service that has this>:
        image: wongwill86/air-tasks:<your tag>
    ```
4. Run [Tests](#testing)

## Debug tools:
[AirFlow](http://localhost) - Airflow Webserver

[Celery Flower](http://localhost/flower) - Monitor Workers

[Swarm Visualizer](http://localhost/visualizer) - Visualize Stack Deployment

[RabbitMQ](http://localhost/rabbitmq) - RabbitMQ Management Plugin (Queue Info)

Note: if running with ssl, use https: instead of http

## Setup:
1. Install docker
	```
	wget -qO- https://get.docker.com/ | sh
	```
2. Install docker compose
    ```
    pip install docker-compose
    ```
## Deploy
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
Use [Cloudformation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new) to create a new stack.
Use this [template](https://raw.githubusercontent.com/wongwill86/examples/air-tasks/latest/swarm/aws/vpc.cfn)

## NOTES:
Chunkflow: make sure AWS_ACCESS_KEY_ID, etc... are set in environment variables!
