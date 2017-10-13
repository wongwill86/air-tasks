# air-tasks

DooD support and AWS ECR Credential Helper

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
[localhost:80](http://localhost) - Airflow Webserver

[localhost:81](http://localhost) - Celery Flower (Monitor Workers)

[localhost:82](http://localhost) - Swarm Visualizer (Visualize Stack Deployment)

[localhost:83](http://localhost) - RabbitMQ Management Plugin (Queue Info)

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
Use this [template](https://raw.githubusercontent.com/wongwill86/examples/air_tasks_rebase/latest/swarm/aws/vpc.cfn)

## NOTES:
Chunkflow: make sure AWS_ACCESS_KEY_ID, etc... are set in environment variables!
