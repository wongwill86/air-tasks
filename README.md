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
#### AWS
1. Optional: Initialize submodule
	```
	git submodule update --recursive --remote
	```
2. Use [Cloudformation](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/new) to create a new stack.
3. Use this [cloud/latest/swarm/aws/vpc.cfn](https://raw.githubusercontent.com/wongwill86/examples/air-tasks/latest/swarm/aws/vpc.cfn)

#### GCloud
1. Optional: Initialize submodule
	```
	git submodule update --recursive --remote
	```
2. Install [Gcloud](https://cloud.google.com/sdk/downloads)
3. Optional: configure yaml (cloud/latest/swarm/google/cloud-deployment.yaml)
4. Deploy on gcloud
	```
	gcloud deployment-manager deployments create <deployment name> --config cloud/latest/swarm/google/cloud-deployment.yaml
	```

## NOTES:
Chunkflow: make sure AWS_ACCESS_KEY_ID, etc... are set in environment variables!
